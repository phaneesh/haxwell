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

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import com.hbase.haxwell.api.HaxwellEventListener;
import com.hbase.haxwell.api.core.HaxwellRow;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.util.List;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

public class HaxwellEventExecutor {

    private Log log = LogFactory.getLog(getClass());
    private HaxwellEventListener eventListener;
    private int numThreads;
    private int batchSize;
    private HaxwellMetrics HaxwellMetrics;
    private List<ThreadPoolExecutor> executors;
    private Multimap<Integer, HaxwellRow> eventBuffers;
    private List<Future<?>> futures;
    private HashFunction hashFunction = Hashing.murmur3_32();
    private boolean stopped = false;

    public HaxwellEventExecutor(HaxwellEventListener eventListener, List<ThreadPoolExecutor> executors, int batchSize, HaxwellMetrics haxwellMetrics) {
        this.eventListener = eventListener;
        this.executors = executors;
        this.numThreads = executors.size();
        this.batchSize = batchSize;
        this.HaxwellMetrics = haxwellMetrics;
        eventBuffers = ArrayListMultimap.create(numThreads, batchSize);
        futures = Lists.newArrayList();
    }

    public void scheduleHaxwellEvent(HaxwellRow haxwellRow) {

        if (stopped) {
            throw new IllegalStateException("This executor is stopped");
        }

        // We don't want messages of the same row to be processed concurrently, therefore choose
        // a thread based on the hash of the row key
        int partition = (hashFunction.hashString(haxwellRow.getId()).asInt() & Integer.MAX_VALUE) % numThreads;
        List<HaxwellRow> eventBuffer = (List<HaxwellRow>) eventBuffers.get(partition);
        eventBuffer.add(haxwellRow);
        if (eventBuffer.size() == batchSize) {
            scheduleEventBatch(partition, Lists.newArrayList(eventBuffer));
            eventBuffers.removeAll(partition);
        }
    }

    private void scheduleEventBatch(int partition, final List<HaxwellRow> events) {
        Future<?> future = executors.get(partition).submit(() -> {
            try {
                long before = System.currentTimeMillis();
                log.debug("Delivering message to listener");
                eventListener.processEvents(events);
                HaxwellMetrics.reportFilteredOperation(System.currentTimeMillis() - before);
            } catch (RuntimeException e) {
                log.error("Error while processing event", e);
                throw e;
            }
        });
        futures.add(future);
    }

    public List<Future<?>> flush() {
        for (int partition : eventBuffers.keySet()) {
            List<HaxwellRow> buffer = (List<HaxwellRow>) eventBuffers.get(partition);
            if (!buffer.isEmpty()) {
                scheduleEventBatch(partition, Lists.newArrayList(buffer));
            }
        }
        eventBuffers.clear();
        return Lists.newArrayList(futures);
    }
}
