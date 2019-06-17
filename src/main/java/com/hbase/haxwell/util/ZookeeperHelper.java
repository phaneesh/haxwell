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
package com.hbase.haxwell.util;

import com.hbase.haxwell.api.ZkConnectException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.ACL;
import org.apache.zookeeper.data.Stat;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class ZookeeperHelper implements Closeable {

  private final Object connectedMonitor = new Object();
  private ZooKeeper delegate;
  private boolean connected = false;
  private volatile boolean stop = false;
  private Thread zkEventThread;

  private Log log = LogFactory.getLog(getClass());

  private ZookeeperHelper() {

  }

  public ZookeeperHelper(String connectString, int sessionTimeout) throws ZkConnectException {
    connect(connectString, sessionTimeout);
  }

  public void connect(String connectString, int sessionTimeout) throws ZkConnectException {
    try {
      this.delegate = new ZooKeeper(connectString, sessionTimeout, null);
    } catch (IOException e) {
      throw new ZkConnectException("Failed to connect with Zookeeper @ '" + connectString + "'", e);
    }
    long waitUntil = System.currentTimeMillis() + sessionTimeout;
    boolean connected = (ZooKeeper.States.CONNECTED).equals(delegate.getState());
    while (!connected && waitUntil > System.currentTimeMillis()) {
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        connected = (ZooKeeper.States.CONNECTED).equals(delegate.getState());
        break;
      }
      connected = (ZooKeeper.States.CONNECTED).equals(delegate.getState());
    }
    if (!connected) {
      System.out.println("Failed to connect to Zookeeper within timeout: Dumping stack: ");
      Thread.dumpStack();
      try {
        delegate.close();
      } catch (InterruptedException e) {
        System.out.println("Failed to close connection: " + e.getMessage());
      }
      throw new ZkConnectException("Failed to connect with Zookeeper @ '" + connectString +
          "' within timeout " + sessionTimeout);
    }
  }

  @Override
  public void close() {
    try {
      delegate.close();
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RuntimeException(e);
    }
  }

  public String create(String path, byte[] data, List<ACL> acl, CreateMode createMode) throws KeeperException, InterruptedException {
    return delegate.create(path, data, acl, createMode);
  }

  public void delete(String path, int version) throws InterruptedException, KeeperException {
    delegate.delete(path, version);
  }

  public List<String> getChildren(String path, boolean watch) throws KeeperException, InterruptedException {
    return delegate.getChildren(path, watch);
  }

  public void createPath(final String path) throws InterruptedException, KeeperException {
    createPath(path, null);
  }

  public void createPath(final String path, final byte[] data)
      throws InterruptedException, KeeperException {
    if (!path.startsWith("/"))
      throw new IllegalArgumentException("Path should start with a slash.");

    if (path.endsWith("/"))
      throw new IllegalArgumentException("Path should not end on a slash.");

    String[] parts = path.substring(1).split("/");

    final StringBuilder subPath = new StringBuilder();
    boolean created = false;
    for (int i = 0; i < parts.length; i++) {
      String part = parts[i];
      subPath.append("/").append(part);

      // Only use the supplied data for the last node in the path
      final byte[] newData = (i == parts.length - 1 ? data : null);

      created = retryOperation(() -> {
        if (delegate.exists(subPath.toString(), false) == null) {
          try {
            delegate.create(subPath.toString(), newData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
            return true;
          } catch (KeeperException.NodeExistsException e) {
            return false;
          }
        }
        return false;
      });
    }

    if (!created) {
      // The node already existed, update its data if necessary
      retryOperation((ZooKeeperOperation<Boolean>) () -> {
        byte[] currentData = delegate.getData(path, false, new Stat());
        if (!Arrays.equals(currentData, data)) {
          delegate.setData(path, data, -1);
        }
        return null;
      });
    }
  }

  public <T> T retryOperation(ZooKeeperOperation<T> operation) throws InterruptedException, KeeperException {
    if (isCurrentThreadEventThread()) {
      throw new RuntimeException("retryOperation should not be called from within the ZooKeeper event thread.");
    }

    int tryCount = 0;

    while (true) {
      tryCount++;

      try {
        return operation.execute();
      } catch (KeeperException.ConnectionLossException e) {
        // ok
      }

      if (tryCount > 3) {
        log.warn("ZooKeeper operation attempt " + tryCount + " failed due to connection loss.");
      }

      waitForConnection();
    }
  }

  public boolean isCurrentThreadEventThread() {
    // Disclaimer: this way of detected wrong use of the event thread was inspired by the ZKClient library.
    return zkEventThread != null && zkEventThread == Thread.currentThread();
  }

  public void waitForConnection() throws InterruptedException {
    if (isCurrentThreadEventThread()) {
      throw new RuntimeException("waitForConnection should not be called from within the ZooKeeper event thread.");
    }

    synchronized (connectedMonitor) {
      while (!connected && !stop) {
        connectedMonitor.wait();
      }
    }

    if (stop) {
      throw new InterruptedException("This ZooKeeper handle is shutting down.");
    }
  }

  public void deleteNode(final String path) throws InterruptedException,
      KeeperException {
    retryOperation(() -> {
      Stat stat = delegate.exists(path, false);
      if (stat != null) {
        try {
          delegate.delete(path, stat.getVersion());
        } catch (KeeperException.NoNodeException nne) {
          // This is ok, the node is already gone
        }
        // We don't catch BadVersion or NotEmpty as these are probably signs that there is something
        // unexpected going on with the node that is to be deleted
      }
      return true;
    });
  }
}
