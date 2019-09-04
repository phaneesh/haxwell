/*
 * Copyright 2019 Phaneesh Nagaraja <phaneesh.n@gmail.com>.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hbase.haxwell;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.protobuf.RpcController;
import com.google.protobuf.ServiceException;
import com.hbase.haxwell.api.HaxwellEventListener;
import com.hbase.haxwell.api.HaxwellSubscription;
import com.hbase.haxwell.api.WaitPolicy;
import com.hbase.haxwell.api.core.HaxwellRow;
import com.hbase.haxwell.api.core.HaxwellRow.OperationType;
import com.hbase.haxwell.util.CloseableHelper;
import com.hbase.haxwell.util.ZookeeperHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellScanner;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValue.Type;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.ServerName;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ipc.FifoRpcScheduler;
import org.apache.hadoop.hbase.ipc.PayloadCarryingRpcController;
import org.apache.hadoop.hbase.ipc.RpcServer;
import org.apache.hadoop.hbase.protobuf.generated.AdminProtos;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.zookeeper.ZKUtil;
import org.apache.hadoop.hbase.zookeeper.ZooKeeperWatcher;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class HaxwellConsumer extends HaxwellRegionServer {

  private final String subscriptionId;
  private final ZookeeperHelper zk;
  private final Configuration hbaseConf;
  boolean running = false;
  private long subscriptionTimestamp;
  private HaxwellEventListener listener;
  private RpcServer rpcServer;
  private ServerName serverName;
  private ZooKeeperWatcher zkWatcher;
  private HaxwellMetrics haxwellMetrics;
  private String zkNodePath;
  private List<ThreadPoolExecutor> executors;
  private int threadCount;
  private int batchSize;
  private long waitTimeout;
  private int executorQueueSize;
  private Log log = LogFactory.getLog(getClass());

  public HaxwellConsumer(String subscriptionId, long subscriptionTimestamp, HaxwellEventListener listener,
                         String hostName, ZookeeperHelper zk, Configuration hbaseConf) throws IOException, InterruptedException {
    this.threadCount = hbaseConf.getInt("hbase.haxwell.consumers.handler.count", 10);
    this.batchSize = hbaseConf.getInt("hbase.haxwell.consumers.events.batchsize", 100);
    this.waitTimeout = hbaseConf.getLong("hbase.haxwell.consumers.execution.timeout.ms", -1);
    this.executorQueueSize = hbaseConf.getInt("hbase.haxwell.consumers.handler.queue.size", 100);
    this.subscriptionId = HaxwellSubscriptionImpl.toInternalSubscriptionName(subscriptionId);
    this.subscriptionTimestamp = subscriptionTimestamp;
    this.listener = listener;
    this.zk = zk;
    this.hbaseConf = hbaseConf;
    this.haxwellMetrics = new HaxwellMetrics(subscriptionId);
    this.executors = Lists.newArrayListWithCapacity(threadCount);

    if(hostName == null && hostName.isEmpty()) {
      hostName = InetAddress.getLocalHost().getHostName();
    }

    InetSocketAddress initialIsa = new InetSocketAddress(hostName, 0);
    if (initialIsa.getAddress() == null) {
      throw new IllegalArgumentException("Failed resolve of " + initialIsa);
    }
    String name = "regionserver/" + initialIsa.toString();
    this.rpcServer = new RpcServer(this, name, getServices(),
        initialIsa, hbaseConf,
        new FifoRpcScheduler(hbaseConf, hbaseConf.getInt("hbase.regionserver.handler.count", 10)));
    this.serverName = ServerName.valueOf(hostName, rpcServer.getListenerAddress().getPort(), System.currentTimeMillis());
    this.zkWatcher = new ZooKeeperWatcher(hbaseConf, this.serverName.toString(), null);

    // login the zookeeper client principal (if using security)
    ZKUtil.loginClient(hbaseConf, "hbase.zookeeper.client.keytab.file",
        "hbase.zookeeper.client.kerberos.principal", hostName);

    // login the server principal (if using secure Hadoop)
    User.login(hbaseConf, "hbase.regionserver.keytab.file",
        "hbase.regionserver.kerberos.principal", hostName);

    for (int i = 0; i < threadCount; i++) {
      ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
          new ArrayBlockingQueue<>(executorQueueSize),
          new ThreadFactoryBuilder().setNameFormat("haxwell-consumer-" + i + "-%d").build());
      if (waitTimeout < 0) {
        executor.setRejectedExecutionHandler(new WaitPolicy());
      } else {
        executor.setRejectedExecutionHandler(new WaitPolicy(waitTimeout, TimeUnit.MILLISECONDS));
      }
      executors.add(executor);
    }
  }

  private List<RpcServer.BlockingServiceAndInterface> getServices() {
    List<RpcServer.BlockingServiceAndInterface> bssi = new ArrayList<>(1);
    bssi.add(new RpcServer.BlockingServiceAndInterface(
        AdminProtos.AdminService.newReflectiveBlockingService(this),
        AdminProtos.AdminService.BlockingInterface.class));
    return bssi;
  }

  public void start() throws InterruptedException, KeeperException {
    rpcServer.start();
    // Publish our existence in ZooKeeper
    zkNodePath = hbaseConf.get(HaxwellSubscription.ZK_ROOT_NODE_CONF_KEY, HaxwellSubscription.DEFAULT_ZK_ROOT_NODE)
        + "/" + subscriptionId + "/rs/" + serverName.getServerName();
    zk.create(zkNodePath, null, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
    this.running = true;
  }

  public void stop() {
    CloseableHelper.close(zkWatcher);
    if (running) {
      running = false;
      CloseableHelper.close(rpcServer);
      try {
        zk.delete(zkNodePath, -1);
      } catch (Exception e) {
        log.debug("Exception while removing zookeeper node", e);
        if (e instanceof InterruptedException) {
          Thread.currentThread().interrupt();
        }
      }
    }
    haxwellMetrics.shutdown();
    for (ThreadPoolExecutor executor : executors) {
      executor.shutdown();
    }
  }

  public boolean isRunning() {
    return running;
  }

  @Override
  public Configuration getConfiguration() {
    return hbaseConf;
  }

  @Override
  public ZooKeeperWatcher getZooKeeper() {
    return zkWatcher;
  }

  @Override
  public ServerName getServerName() {
    return serverName;
  }

  @Override
  public AdminProtos.ReplicateWALEntryResponse replicateWALEntry(final RpcController controller,
                                                                 final AdminProtos.ReplicateWALEntryRequest request) throws ServiceException {
    try {
      long lastProcessedTimestamp = -1;
      HaxwellEventExecutor eventExecutor = new HaxwellEventExecutor(listener, executors, batchSize, haxwellMetrics);
      List<AdminProtos.WALEntry> entries = request.getEntryList();

      CellScanner cells = ((PayloadCarryingRpcController) controller).cellScanner();
      for (final AdminProtos.WALEntry entry : entries) {
        TableName tableName = (entry.getKey().getWriteTime() < subscriptionTimestamp) ? null :
            TableName.valueOf(entry.getKey().getTableName().toByteArray());
        Multimap<ByteBuffer, Cell> keyValuesPerRowKey = ArrayListMultimap.create();
        int count = entry.getAssociatedCellCount();
        OperationType operationType = OperationType.UNKNOWN;
        for (int i = 0; i < count; i++) {
          if (!cells.advance()) {
            throw new ArrayIndexOutOfBoundsException("Expected=" + count + ", index=" + i);
          }
          // this signals to us that we simply need to skip over count of cells
          if (tableName == null) {
            continue;
          }
          Cell cell = cells.current();
          if (cell.getTypeByte() == Type.Put.getCode()) {
            operationType = OperationType.PUT;
          }
          if (cell.getTypeByte() == Type.Delete.getCode()) {
            operationType = OperationType.DELETE;
          }
          if (operationType != null) {
            ByteBuffer rowKey = ByteBuffer.wrap(cell.getRowArray(), cell.getRowOffset(), cell.getRowLength());
            KeyValue kv = KeyValueUtil.ensureKeyValue(cell);
            keyValuesPerRowKey.put(rowKey, kv);
          }
        }
        for (final ByteBuffer rowKeyBuffer : keyValuesPerRowKey.keySet()) {
          final List<Cell> keyValues = (List<Cell>) keyValuesPerRowKey.get(rowKeyBuffer);
          final HaxwellRow row = HaxwellRow.create(tableName.getName(), CellUtil.cloneRow(keyValues.get(0)),
              keyValues, operationType, entry.getKey().getWriteTime());
          eventExecutor.scheduleHaxwellEvent(row);
          lastProcessedTimestamp = Math.max(lastProcessedTimestamp, entry.getKey().getWriteTime());
        }

      }
      List<Future<?>> futures = eventExecutor.flush();
      waitOnHaxwellSubscriptionCompletion(futures);
      if (lastProcessedTimestamp > 0) {
        haxwellMetrics.reportEventTimestamp(lastProcessedTimestamp);
      }
      return AdminProtos.ReplicateWALEntryResponse.newBuilder().build();
    } catch (IOException ie) {
      throw new ServiceException(ie);
    }
  }

  private void waitOnHaxwellSubscriptionCompletion(List<Future<?>> futures) throws IOException {
    List<Exception> exceptionsThrown = Lists.newArrayList();
    for (Future<?> future : futures) {
      try {
        future.get();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new IOException("Interrupted in processing events.", e);
      } catch (Exception e) {
        log.warn("Error processing a batch of events, the error will be forwarded to HBase for retry", e);
        exceptionsThrown.add(e);
      }
    }

    if (!exceptionsThrown.isEmpty()) {
      log.error("Encountered exceptions on " + exceptionsThrown.size() + " batches (out of " + futures.size()
          + " total batches)");
      throw new RuntimeException(exceptionsThrown.get(0));
    }
  }
}
