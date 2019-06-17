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

import com.hbase.haxwell.util.ZookeeperHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;

public class HaxwellSubscriptionTest {

  @Test(expected = IllegalStateException.class)
  public void testInstantiate_InvalidZkQuorumString() {
    Configuration conf = HBaseConfiguration.create();

    conf.set("hbase.zookeeper.quorum", "host:2181");

    new HaxwellSubscriptionImpl(mock(ZookeeperHelper.class), conf);
  }

  @Test(expected = IllegalStateException.class)
  public void testInstantiate_NonNumericZkClientPort() {
    Configuration conf = HBaseConfiguration.create();

    conf.set("hbase.zookeeper.property.clientPort", "not a number");

    new HaxwellSubscriptionImpl(mock(ZookeeperHelper.class), conf);
  }


  @Test
  public void testToInternalSubscriptionName_NoSpecialCharacters() {
    assertEquals("subscription_name", HaxwellSubscriptionImpl.toInternalSubscriptionName("subscription_name"));
  }

  @Test
  public void testToInternalSubscriptionName_HyphenMapping() {
    assertEquals("subscription" + HaxwellSubscriptionImpl.INTERNAL_HYPHEN_REPLACEMENT + "name", HaxwellSubscriptionImpl.toInternalSubscriptionName("subscription-name"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testToInternalSubscriptionName_NameContainsMappedHyphen() {
    // We can't allow the internal hyphen replacement to ever be present on an external name,
    // otherwise we could get duplicate mapped names
    HaxwellSubscriptionImpl.toInternalSubscriptionName("subscription" + HaxwellSubscriptionImpl.INTERNAL_HYPHEN_REPLACEMENT + "name");
  }
}
