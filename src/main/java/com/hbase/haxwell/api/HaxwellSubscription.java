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
package com.hbase.haxwell.api;

import org.apache.zookeeper.KeeperException;

import java.io.IOException;

public interface HaxwellSubscription {

  String ZK_ROOT_NODE_CONF_KEY = "haxwell.zookeeper.znode.parent";

  String DEFAULT_ZK_ROOT_NODE = "/haxwell/hbase-slave";

  void addSubscription(String name) throws InterruptedException, KeeperException, IOException;

  boolean addSubscriptionSilent(String name) throws InterruptedException, KeeperException, IOException;

  void removeSubscription(String name) throws IOException;

  boolean removeSubscriptionSilent(String name) throws IOException;

  boolean hasSubscription(String name) throws IOException;
}
