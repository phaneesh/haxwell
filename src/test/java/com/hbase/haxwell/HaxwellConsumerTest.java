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
import com.hbase.haxwell.api.HaxwellEvent;
import com.hbase.haxwell.api.HaxwellEventListener;
import com.hbase.haxwell.util.ZookeeperHelper;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.protobuf.ReplicationProtbufUtil;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.wal.WAL;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

import static org.mockito.Mockito.*;

public class HaxwellConsumerTest {

    private static final long SUBSCRIPTION_TIMESTAMP = 100000;

    private static final byte[] TABLE_NAME = Bytes.toBytes("test_table");
    private static final byte[] DATA_COLFAM = Bytes.toBytes("data");
    private static final byte[] PAYLOAD_QUALIFIER = Bytes.toBytes("pl");
    private static final byte[] encodedRegionName = Bytes.toBytes("1028785192");
    private static final List<UUID> clusterUUIDs = new ArrayList<>();

    private HaxwellEventListener eventListener;
    private ZookeeperHelper zookeeperHelper;
    private HaxwellConsumer haxwellConsumer;

    @Before
    public void setUp() throws IOException, InterruptedException {
        eventListener = mock(HaxwellEventListener.class);
        zookeeperHelper = mock(ZookeeperHelper.class);
        haxwellConsumer = new HaxwellConsumer("subscriptionId", SUBSCRIPTION_TIMESTAMP, eventListener, "localhost", zookeeperHelper,
                HBaseConfiguration.create());
    }

    @After
    public void tearDown() {
        haxwellConsumer.stop();
    }

    private WAL.Entry createHlogEntry(byte[] tableName, Cell... keyValues) {
        return createHlogEntry(tableName, SUBSCRIPTION_TIMESTAMP + 1, keyValues);
    }

    private WAL.Entry createHlogEntry(byte[] tableName, long writeTime, Cell... keyValues) {
        WAL.Entry entry = mock(WAL.Entry.class, Mockito.RETURNS_DEEP_STUBS);
        when(entry.getEdit().getCells()).thenReturn(Lists.newArrayList(keyValues));
        when(entry.getKey().getTableName()).thenReturn(TableName.valueOf(tableName));
        when(entry.getKey().getWriteTime()).thenReturn(writeTime);
        when(entry.getKey().getEncodedRegionName()).thenReturn(encodedRegionName);
        when(entry.getKey().getClusterIds()).thenReturn(clusterUUIDs);
        return entry;
    }

    @Test
    public void testReplicateLogEntries() throws IOException {

        byte[] rowKey = Bytes.toBytes("rowkey");

        WAL.Entry hlogEntry = createHlogEntry(TABLE_NAME, new KeyValue(rowKey, DATA_COLFAM,
                PAYLOAD_QUALIFIER));

        replicateWALEntry(new WAL.Entry[]{hlogEntry});

        HaxwellEvent expectedHaxwellEvent = HaxwellEvent.create(TABLE_NAME, rowKey,
                hlogEntry.getEdit().getCells());

        verify(eventListener).processEvents(Lists.newArrayList(expectedHaxwellEvent));
    }

    @Test
    public void testReplicateLogEntries_EntryTimestampBeforeSubscriptionTimestamp() throws IOException {
        byte[] rowKey = Bytes.toBytes("rowkey");
        byte[] payloadDataBeforeTimestamp = Bytes.toBytes("payloadBeforeTimestamp");
        byte[] payloadDataOnTimestamp = Bytes.toBytes("payloadOnTimestamp");
        byte[] payloadDataAfterTimestamp = Bytes.toBytes("payloadAfterTimestamp");

        WAL.Entry hlogEntryBeforeTimestamp = createHlogEntry(TABLE_NAME, SUBSCRIPTION_TIMESTAMP - 1,
                new KeyValue(rowKey, DATA_COLFAM, PAYLOAD_QUALIFIER, payloadDataBeforeTimestamp));
        WAL.Entry hlogEntryOnTimestamp = createHlogEntry(TABLE_NAME, SUBSCRIPTION_TIMESTAMP,
                new KeyValue(rowKey, DATA_COLFAM, PAYLOAD_QUALIFIER, payloadDataOnTimestamp));
        WAL.Entry hlogEntryAfterTimestamp = createHlogEntry(TABLE_NAME, SUBSCRIPTION_TIMESTAMP + 1,
                new KeyValue(rowKey, DATA_COLFAM, PAYLOAD_QUALIFIER, payloadDataAfterTimestamp));

        replicateWALEntry(new WAL.Entry[]{hlogEntryBeforeTimestamp});
        replicateWALEntry(new WAL.Entry[]{hlogEntryOnTimestamp});
        replicateWALEntry(new WAL.Entry[]{hlogEntryAfterTimestamp});

        HaxwellEvent expectedEventOnTimestamp = HaxwellEvent.create(TABLE_NAME, rowKey,
                hlogEntryOnTimestamp.getEdit().getCells());
        HaxwellEvent expectedEventAfterTimestamp = HaxwellEvent.create(TABLE_NAME, rowKey,
                hlogEntryAfterTimestamp.getEdit().getCells());

        verify(eventListener, times(2)).processEvents(Lists.newArrayList(expectedEventOnTimestamp));
        verify(eventListener, times(2)).processEvents(Lists.newArrayList(expectedEventAfterTimestamp));
        verifyNoMoreInteractions(eventListener);
    }

    @Test
    public void testReplicateLogEntries_MultipleKeyValuesForSingleRow() throws Exception {
        byte[] rowKey = Bytes.toBytes("rowKey");

        Cell kvA = new KeyValue(rowKey, DATA_COLFAM, PAYLOAD_QUALIFIER, Bytes.toBytes("A"));
        Cell kvB = new KeyValue(rowKey, DATA_COLFAM, PAYLOAD_QUALIFIER, Bytes.toBytes("B"));

        WAL.Entry entry = createHlogEntry(TABLE_NAME, kvA, kvB);

        replicateWALEntry(new WAL.Entry[]{entry});
        HaxwellEvent expectedEvent = HaxwellEvent.create(TABLE_NAME, rowKey, Lists.newArrayList(kvA, kvB));

        verify(eventListener).processEvents(Lists.newArrayList(expectedEvent));
    }

    @Test
    public void testReplicateLogEntries_SingleWALEditForMultipleRows() throws IOException {

        byte[] rowKeyA = Bytes.toBytes("A");
        byte[] rowKeyB = Bytes.toBytes("B");
        byte[] data = Bytes.toBytes("data");

        Cell kvA = new KeyValue(rowKeyA, DATA_COLFAM, PAYLOAD_QUALIFIER, data);
        Cell kvB = new KeyValue(rowKeyB, DATA_COLFAM, PAYLOAD_QUALIFIER, data);

        WAL.Entry entry = createHlogEntry(TABLE_NAME, kvA, kvB);

        replicateWALEntry(new WAL.Entry[]{entry});

        HaxwellEvent expectedEventA = HaxwellEvent.create(TABLE_NAME, rowKeyA, Lists.newArrayList(kvA));
        HaxwellEvent expectedEventB = HaxwellEvent.create(TABLE_NAME, rowKeyB, Lists.newArrayList(kvB));

        verify(eventListener).processEvents(Lists.newArrayList(expectedEventA, expectedEventB));
    }

    private void replicateWALEntry(WAL.Entry[] entries) throws IOException {
        ReplicationProtbufUtil.replicateWALEntry((org.apache.hadoop.hbase.shaded.protobuf.generated.AdminProtos.AdminService.BlockingInterface)haxwellConsumer, entries, null, null, null);
    }
}
