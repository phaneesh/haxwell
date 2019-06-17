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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.hbase.haxwell.api.HaxwellEventListener;
import com.hbase.haxwell.api.HaxwellSubscription;
import com.hbase.haxwell.api.ZkConnectException;
import com.hbase.haxwell.api.core.HaxwellRow;
import com.hbase.haxwell.util.ZookeeperHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import static junit.framework.TestCase.assertEquals;

public class HaxwellIntegrationTest {

  private static final byte[] TABLE_NAME = Bytes.toBytes("test_table");
  private static final byte[] DATA_COL_FAMILY = Bytes.toBytes("datacf");
  private static final byte[] PAYLOAD_COL_FAMILY = Bytes.toBytes("payloadcf");
  private static final byte[] PAYLOAD_COL_QUALIFIER = Bytes.toBytes("payload_qualifier");
  private static final String SUBSCRIPTION_NAME = "test_subscription";
  private static final String SUBSCRIPTION_WITH_PAYLOADS_NAME = "test_subscription_with_payloads";
  private static final int WAIT_TIMEOUT = 10000;

  private static Configuration clusterConf;
  private static HBaseTestingUtility hbaseTestUtil;
  private static Table htable;
  private static Connection connection;

  private ZookeeperHelper zookeeperHelper;
  private HaxwellSubscription haxwellSubscription;
  private HaxwellConsumer haxwellConsumer;
  private HaxwellConsumer haxwellConsumerWithPayloads;
  private TestEventListener eventListener;
  private TestEventListener eventListenerWithPayloads;

  @BeforeClass
  public static void setUpBeforeClass() throws Exception {
    clusterConf = HBaseConfiguration.create();
    clusterConf.setLong("replication.source.sleepforretries", 50);
    clusterConf.set("replication.replicationsource.implementation", HaxwellReplicationSource.class.getName());
    clusterConf.setInt("hbase.master.info.port", -1);
    clusterConf.setInt("hbase.regionserver.info.port", -1);

    hbaseTestUtil = new HBaseTestingUtility(clusterConf);

    hbaseTestUtil.startMiniZKCluster(1);
    hbaseTestUtil.startMiniCluster(1);

    HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));
    HColumnDescriptor dataColfamDescriptor = new HColumnDescriptor(DATA_COL_FAMILY);
    dataColfamDescriptor.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    HColumnDescriptor payloadColfamDescriptor = new HColumnDescriptor(PAYLOAD_COL_FAMILY);
    payloadColfamDescriptor.setScope(HConstants.REPLICATION_SCOPE_GLOBAL);
    tableDescriptor.addFamily(dataColfamDescriptor);
    tableDescriptor.addFamily(payloadColfamDescriptor);

    connection = ConnectionFactory.createConnection(clusterConf);
    connection.getAdmin().createTable(tableDescriptor);
    htable = connection.getTable(TableName.valueOf(TABLE_NAME));
  }

  @AfterClass
  public static void tearDownAfterClass() throws Exception {
    if (connection != null) {
      connection.close();
    }
    hbaseTestUtil.shutdownMiniCluster();
  }

  @Before
  public void setUp() throws ZkConnectException, InterruptedException, KeeperException, IOException {
    zookeeperHelper = new ZookeeperHelper("localhost:" + hbaseTestUtil.getZkCluster().getClientPort(),
        30000);
    haxwellSubscription = new HaxwellSubscriptionImpl(zookeeperHelper, clusterConf);
    haxwellSubscription.addSubscription(SUBSCRIPTION_NAME);
    haxwellSubscription.addSubscription(SUBSCRIPTION_WITH_PAYLOADS_NAME);
    eventListener = new TestEventListener();
    eventListenerWithPayloads = new TestEventListener();
    haxwellConsumer = new HaxwellConsumer(SUBSCRIPTION_NAME, System.currentTimeMillis(), eventListener, "localhost",
        zookeeperHelper, clusterConf);
    haxwellConsumerWithPayloads = new HaxwellConsumer(SUBSCRIPTION_WITH_PAYLOADS_NAME, System.currentTimeMillis(), eventListenerWithPayloads, "localhost", zookeeperHelper, clusterConf);
    haxwellConsumer.start();
    haxwellConsumerWithPayloads.start();
  }

  @After
  public void tearDown() throws IOException {
    haxwellConsumer.stop();
    haxwellConsumerWithPayloads.stop();
    haxwellSubscription.removeSubscription(SUBSCRIPTION_NAME);
    haxwellSubscription.removeSubscription(SUBSCRIPTION_WITH_PAYLOADS_NAME);
    zookeeperHelper.close();
  }

  @Test
  public void testEvents_SimpleSetOfPuts() throws IOException {
    for (int i = 0; i < 3; i++) {
      Put put = new Put(Bytes.toBytes("row " + i));
      put.addColumn(DATA_COL_FAMILY, Bytes.toBytes("qualifier"), Bytes.toBytes("value"));
      htable.put(put);
    }
    waitForEvents(eventListener, 3);
    waitForEvents(eventListenerWithPayloads, 3);

    assertEquals(3, eventListener.getEvents().size());
    assertEquals(3, eventListenerWithPayloads.getEvents().size());

    Set<String> rowKeys = Sets.newHashSet();
    for (HaxwellRow haxwellEvent : eventListener.getEvents()) {
      rowKeys.add(haxwellEvent.getId());
    }
    assertEquals(Sets.newHashSet("row 0", "row 1", "row 2"), rowKeys);
  }

  private void waitForEvents(TestEventListener listener, int expectedNumEvents) {
    long start = System.currentTimeMillis();
    while (listener.getEvents().size() < expectedNumEvents) {
      if (System.currentTimeMillis() - start > WAIT_TIMEOUT) {
        throw new RuntimeException("Waited too long on " + expectedNumEvents + ", only have "
            + listener.getEvents().size() + " after " + WAIT_TIMEOUT + " milliseconds");
      }
      try {
        Thread.sleep(100);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        throw new RuntimeException(e);
      }
    }
  }

  @Test
  public void testEvents_MultipleKeyValuesInSinglePut() throws IOException {
    Put putA = new Put(Bytes.toBytes("rowA"));
    Put putB = new Put(Bytes.toBytes("rowB"));

    putA.addColumn(DATA_COL_FAMILY, Bytes.toBytes("a1"), Bytes.toBytes("valuea1"));
    putA.addColumn(DATA_COL_FAMILY, Bytes.toBytes("a2"), Bytes.toBytes("valuea2"));

    putB.addColumn(DATA_COL_FAMILY, Bytes.toBytes("b1"), Bytes.toBytes("valueb1"));
    putB.addColumn(DATA_COL_FAMILY, Bytes.toBytes("b2"), Bytes.toBytes("valueb2"));
    putB.addColumn(DATA_COL_FAMILY, Bytes.toBytes("b3"), Bytes.toBytes("valueb3"));

    htable.put(putA);
    htable.put(putB);

    waitForEvents(eventListener, 2);
    waitForEvents(eventListenerWithPayloads, 2);

    assertEquals(2, eventListenerWithPayloads.getEvents().size());

    List<HaxwellRow> events = eventListener.getEvents();

    assertEquals(2, events.size());

    HaxwellRow eventA, eventB;
    if ("rowA".equals(events.get(0).getId())) {
      eventA = events.get(0);
      eventB = events.get(1);
    } else {
      eventA = events.get(1);
      eventB = events.get(0);
    }
    assertEquals("rowA", eventA.getId());
    assertEquals("rowB", eventB.getId());
  }

  @Test
  public void testEvents_WithPayload() throws IOException {
    Put put = new Put(Bytes.toBytes("rowkey"));
    put.addColumn(DATA_COL_FAMILY, Bytes.toBytes("data"), Bytes.toBytes("value"));
    put.addColumn(PAYLOAD_COL_FAMILY, PAYLOAD_COL_QUALIFIER, Bytes.toBytes("payload"));
    htable.put(put);

    waitForEvents(eventListener, 1);
    waitForEvents(eventListenerWithPayloads, 1);

    HaxwellRow eventWithoutPayload = eventListener.getEvents().get(0);
    HaxwellRow eventWithPayload = eventListenerWithPayloads.getEvents().get(0);

    assertEquals("rowkey", eventWithoutPayload.getId());
    assertEquals("rowkey", eventWithPayload.getId());

    assertEquals(2, eventWithoutPayload.getColumns().size());
    assertEquals(2, eventWithPayload.getColumns().size());
  }

  static class TestEventListener implements HaxwellEventListener {

    private List<HaxwellRow> haxwellEvents = Collections.synchronizedList(Lists.newArrayList());

    @Override
    public synchronized void processEvents(List<HaxwellRow> events) {
      haxwellEvents.addAll(events);
    }

    public List<HaxwellRow> getEvents() {
      return Collections.unmodifiableList(haxwellEvents);
    }

    public void reset() {
      haxwellEvents.clear();
    }

  }

}
