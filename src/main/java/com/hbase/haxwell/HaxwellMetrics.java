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

import org.apache.hadoop.metrics.MetricsContext;
import org.apache.hadoop.metrics.MetricsRecord;
import org.apache.hadoop.metrics.MetricsUtil;
import org.apache.hadoop.metrics.Updater;
import org.apache.hadoop.metrics.util.MBeanUtil;
import org.apache.hadoop.metrics.util.MetricsBase;
import org.apache.hadoop.metrics.util.MetricsDynamicMBeanBase;
import org.apache.hadoop.metrics.util.MetricsLongValue;
import org.apache.hadoop.metrics.util.MetricsRegistry;
import org.apache.hadoop.metrics.util.MetricsTimeVaryingRate;

import javax.management.ObjectName;


public class HaxwellMetrics implements Updater {

  private final String recordName;
  private final MetricsRegistry metricsRegistry;
  private final MetricsRecord metricsRecord;
  private final MetricsContext context;
  private final HaxwellMetricsMXBean mbean;

  private final MetricsTimeVaryingRate processingRate;

  private final MetricsLongValue lastTimestampInputProcessed;

  public HaxwellMetrics(String recordName) {
    this.recordName = recordName;
    metricsRegistry = new MetricsRegistry();
    processingRate = new MetricsTimeVaryingRate("haxwellProcessed", metricsRegistry);
    lastTimestampInputProcessed = new MetricsLongValue("lastHaxwellTimestamp", metricsRegistry);

    context = MetricsUtil.getContext("repository");
    metricsRecord = MetricsUtil.createRecord(context, recordName);
    context.registerUpdater(this);
    mbean = new HaxwellMetricsMXBean(this.metricsRegistry);
  }

  public void shutdown() {
    context.unregisterUpdater(this);
    mbean.shutdown();
  }

  public void reportFilteredOperation(long duration) {
    processingRate.inc(duration);
  }

  public void reportEventTimestamp(long writeTimestamp) {
    lastTimestampInputProcessed.set(writeTimestamp);
  }

  @Override
  public void doUpdates(MetricsContext unused) {
    synchronized (this) {
      for (MetricsBase m : metricsRegistry.getMetricsList()) {
        m.pushMetric(metricsRecord);
      }
    }
    metricsRecord.update();
  }

  public class HaxwellMetricsMXBean extends MetricsDynamicMBeanBase {
    private final ObjectName mbeanName;

    public HaxwellMetricsMXBean(MetricsRegistry registry) {
      super(registry, "Haxwell Metrics");
      mbeanName = MBeanUtil.registerMBean("haxwell", recordName, this);
    }

    public void shutdown() {
      if (mbeanName != null)
        MBeanUtil.unregisterMBean(mbeanName);
    }
  }
}
