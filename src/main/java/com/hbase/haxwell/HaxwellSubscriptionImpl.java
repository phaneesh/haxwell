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

import com.hbase.haxwell.api.HaxwellSubscription;
import com.hbase.haxwell.util.CloseableHelper;
import com.hbase.haxwell.util.ZookeeperHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.replication.ReplicationAdmin;
import org.apache.hadoop.hbase.replication.ReplicationPeerConfig;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.zookeeper.KeeperException;

import java.io.IOException;
import java.util.UUID;

public class HaxwellSubscriptionImpl implements HaxwellSubscription {

    public static final char INTERNAL_HYPHEN_REPLACEMENT = '\u1400';

    private final ZookeeperHelper zk;
    private final Configuration hbaseConf;
    private final String baseZkPath;
    private final String zkQuorumString;
    private final int zkClientPort;
    private Log log = LogFactory.getLog(getClass());

    public HaxwellSubscriptionImpl(ZookeeperHelper zk, Configuration hbaseConf) {

        this.zkQuorumString = hbaseConf.get("hbase.zookeeper.quorum");
        if (zkQuorumString == null) {
            throw new IllegalStateException("hbase.zookeeper.quorum not supplied in configuration");
        }
        if (zkQuorumString.contains(":")) {
            throw new IllegalStateException("hbase.zookeeper.quorum should not include port number, got " + zkQuorumString);
        }
        try {
            this.zkClientPort = Integer.parseInt(hbaseConf.get("hbase.zookeeper.property.clientPort"));
        } catch (NumberFormatException e) {
            throw new IllegalStateException("Non-numeric zookeeper client port", e);
        }

        this.zk = zk;
        this.hbaseConf = hbaseConf;
        this.baseZkPath = hbaseConf.get(ZK_ROOT_NODE_CONF_KEY, DEFAULT_ZK_ROOT_NODE);
    }

    static String toInternalSubscriptionName(String subscriptionName) {
        if (subscriptionName.indexOf(INTERNAL_HYPHEN_REPLACEMENT, 0) != -1) {
            throw new IllegalArgumentException("Subscription name cannot contain character \\U1400");
        }
        return subscriptionName.replace('-', INTERNAL_HYPHEN_REPLACEMENT);
    }

    static String toExternalSubscriptionName(String subscriptionName) {
        return subscriptionName.replace(INTERNAL_HYPHEN_REPLACEMENT, '-');
    }

    @Override
    public void addSubscription(String name) throws InterruptedException, KeeperException, IOException {
        if (!addSubscriptionSilent(name)) {
            throw new IllegalStateException("There is already a subscription for name '" + name + "'.");
        }
    }

    @Override
    public boolean addSubscriptionSilent(String name) throws InterruptedException, KeeperException, IOException {
        Admin replicationAdmin = ConnectionFactory.createConnection(hbaseConf).getAdmin();
        try {
            String internalName = toInternalSubscriptionName(name);
            if (replicationAdmin.listReplicationPeers().stream().anyMatch(r -> r.getPeerId().equals(internalName))) {
                return false;
            }

            String basePath = baseZkPath + "/" + internalName;
            UUID uuid = UUID.nameUUIDFromBytes(Bytes.toBytes(internalName)); // always gives the same uuid for the same name
            zk.createPath(basePath + "/hbaseid", Bytes.toBytes(uuid.toString()));
            zk.createPath(basePath + "/rs");
            try {
                ReplicationPeerConfig peerConfig = ReplicationPeerConfig.newBuilder()
                        .setClusterKey(uuid.toString()).build();
                replicationAdmin.addReplicationPeer(internalName, peerConfig, true);
            } catch (IllegalArgumentException e) {
                if (e.getMessage().equals("Cannot add existing peer")) {
                    return false;
                }
                throw e;
            } catch (Exception e) {
                throw new IOException(e);
            }
            return true;
        } finally {
            CloseableHelper.close(replicationAdmin);
        }
    }

    @Override
    public void removeSubscription(String name) throws IOException {
        if (!removeSubscriptionSilent(name)) {
            throw new IllegalStateException("No subscription named '" + name + "'.");
        }
    }

    @Override
    public boolean removeSubscriptionSilent(String name) throws IOException {
        ReplicationAdmin replicationAdmin = new ReplicationAdmin(hbaseConf);
        try {
            String internalName = toInternalSubscriptionName(name);
            if (!replicationAdmin.listPeerConfigs().containsKey(internalName)) {
                log.error("Requested to remove a subscription which does not exist, skipping silently: '" + name + "'");
                return false;
            } else {
                try {
                    replicationAdmin.removePeer(internalName);
                } catch (IllegalArgumentException e) {
                    if (e.getMessage().equals("Cannot remove inexisting peer")) { // see ReplicationZookeeper
                        return false;
                    }
                    throw e;
                } catch (Exception e) {
                    // HBase 0.95+ throws at least one extra exception: ReplicationException which we convert into IOException
                    throw new IOException(e);
                }
            }
            String basePath = baseZkPath + "/" + internalName;
            try {
                zk.deleteNode(basePath + "/hbaseid");
                for (String child : zk.getChildren(basePath + "/rs", false)) {
                    zk.deleteNode(basePath + "/rs/" + child);
                }
                zk.deleteNode(basePath + "/rs");
                zk.deleteNode(basePath);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(ie);
            } catch (KeeperException ke) {
                log.error("Cleanup in zookeeper failed on " + basePath, ke);
            }
            return true;
        } finally {
            CloseableHelper.close(replicationAdmin);
        }
    }

    @Override
    public boolean hasSubscription(String name) throws IOException {
        ReplicationAdmin replicationAdmin = new ReplicationAdmin(hbaseConf);
        try {
            String internalName = toInternalSubscriptionName(name);
            return replicationAdmin.listPeerConfigs().containsKey(internalName);
        } finally {
            CloseableHelper.close(replicationAdmin);
        }
    }
}
