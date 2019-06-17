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
package com.hbase.haxwell.demo;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.UUID;

public class HaxwellDemoIngester {

  public static void main(String[] args) throws Exception {
    run();
  }

  private static void run() throws Exception {
    Configuration conf = HBaseConfiguration.create();
    createSchema(conf);
    final byte[] infoCf = Bytes.toBytes("info");
    final byte[] payloadCq = Bytes.toBytes("payload");
    Table htable = ConnectionFactory.createConnection(conf).getTable(TableName.valueOf("haxwell-demo"));

    for (int i = 0; i < 1000; i++) {
      byte[] rowkey = Bytes.toBytes(UUID.randomUUID().toString());
      Put put = new Put(rowkey);
      put.addColumn(infoCf, payloadCq, Bytes.toBytes("test " + i));
      htable.put(put);
      System.out.println("Added row " + Bytes.toString(rowkey));
    }
  }

  public static void createSchema(Configuration hbaseConf) throws IOException {
    Admin admin = ConnectionFactory.createConnection(hbaseConf).getAdmin();
    if (!admin.tableExists(TableName.valueOf("haxwell-demo"))) {
      HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf("haxwell-demo"));
      HColumnDescriptor infoCf = new HColumnDescriptor("info");
      infoCf.setScope(1);
      tableDescriptor.addFamily(infoCf);
      admin.createTable(tableDescriptor);
    }
    admin.close();
  }

}
