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

import com.hbase.haxwell.HaxwellConsumer;
import com.hbase.haxwell.HaxwellSubscriptionImpl;
import com.hbase.haxwell.api.HaxwellEventListener;
import com.hbase.haxwell.api.HaxwellSubscription;
import com.hbase.haxwell.api.core.HaxwellRow;
import com.hbase.haxwell.util.ZookeeperHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;

import java.io.IOException;
import java.util.List;

public class HaxwellDemo {

    public static void main(String[] args) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        createSchema(conf);
        ZookeeperHelper zk = new ZookeeperHelper("localhost:2181", 20000);
        HaxwellSubscription haxwellSubscription = new HaxwellSubscriptionImpl(zk, conf);

        final String subscriptionName = "haxwell_demo";

        if (!haxwellSubscription.hasSubscription(subscriptionName)) {
            haxwellSubscription.addSubscriptionSilent(subscriptionName);
        }
        HaxwellConsumer haxwellConsumer = new HaxwellConsumer(subscriptionName, 0, new EventLogger(), "localhost", zk, conf);

        haxwellConsumer.start();
        System.out.println("Started");

        while (true) {
            Thread.sleep(Long.MAX_VALUE);
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


    private static class EventLogger implements HaxwellEventListener {
        @Override
        public void processEvents(List<HaxwellRow> haxwellRows) {
            for (HaxwellRow haxwellRow : haxwellRows) {
                System.out.println("Received event:");
                System.out.println(haxwellRow.toString());
            }
        }
    }
}
