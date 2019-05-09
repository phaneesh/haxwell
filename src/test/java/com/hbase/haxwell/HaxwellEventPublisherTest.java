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

import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.Before;
import org.junit.Test;
import org.mockito.ArgumentCaptor;

import java.io.IOException;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertArrayEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

public class HaxwellEventPublisherTest {

    private static final byte[] PAYLOAD_CF = Bytes.toBytes("payload column family");
    private static final byte[] PAYLOAD_CQ = Bytes.toBytes("payload column qualifier");

    private Table recordTable;
    private HaxwellDefaultEventPublisher eventPublisher;

    @Before
    public void setUp() {
        recordTable = mock(Table.class);
        eventPublisher = new HaxwellDefaultEventPublisher(recordTable, PAYLOAD_CF, PAYLOAD_CQ);
    }

    @Test
    public void testPublishEvent() throws IOException {
        byte[] eventRow = Bytes.toBytes("row-id");
        byte[] eventPayload = Bytes.toBytes("payload");

        ArgumentCaptor<Put> putCaptor = ArgumentCaptor.forClass(Put.class);

        eventPublisher.publishEvent(eventRow, eventPayload);

        verify(recordTable).put(putCaptor.capture());
        Put put = putCaptor.getValue();

        assertArrayEquals(eventRow, put.getRow());
        assertEquals(1, put.size());
        assertArrayEquals(eventPayload, CellUtil.cloneValue(put.get(PAYLOAD_CF, PAYLOAD_CQ).get(0)));
    }
}
