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
import com.hbase.haxwell.api.HaxwellEventListener;
import com.hbase.haxwell.api.WaitPolicy;
import com.hbase.haxwell.api.core.HaxwellRow;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class HaxwellExecutorTest {

    private HaxwellMetrics haxwellMetrics;
    private List<ThreadPoolExecutor> executors;

    @Before
    public void setUp() {
        haxwellMetrics = mock(HaxwellMetrics.class);
        executors = Lists.newArrayListWithCapacity(10);
        for (int i = 0; i < 10; i++) {
            ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1, 10, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<>(100));
            executor.setRejectedExecutionHandler(new WaitPolicy());
            executors.add(executor);
        }
    }

    @After
    public void tearDown() {
        for (ThreadPoolExecutor executor : executors) {
            executor.shutdownNow();
        }
    }

    private HaxwellRow createHaxwellSubscription(int row) {
        HaxwellRow haxwellEvent = mock(HaxwellRow.class);
        when(haxwellEvent.getId()).thenReturn(String.valueOf(row));
        return haxwellEvent;
    }

    private List<ThreadPoolExecutor> getExecutors(int numThreads) {
        return executors.subList(0, numThreads);
    }

    @Test
    public void testScheduleHaxwellSubscription() throws InterruptedException {
        RecordingEventListener eventListener = new RecordingEventListener();
        HaxwellEventExecutor executor = new HaxwellEventExecutor(eventListener, getExecutors(2), 1, haxwellMetrics);
        final int NUM_EVENTS = 10;
        for (int i = 0; i < NUM_EVENTS; i++) {
            executor.scheduleHaxwellEvent(createHaxwellSubscription(i));
        }

        for (int retry = 0; retry < 50; retry++) {
            if (eventListener.receivedEvents.size() >= NUM_EVENTS) {
                break;
            }
            Thread.sleep(250);
        }

        assertEquals(NUM_EVENTS, eventListener.receivedEvents.size());
    }

    @Test
    public void testScheduleHaxwellSubscription_NotFullBatch() throws InterruptedException {
        RecordingEventListener eventListener = new RecordingEventListener();
        HaxwellEventExecutor executor = new HaxwellEventExecutor(eventListener, getExecutors(2), 100, haxwellMetrics);
        final int NUM_EVENTS = 10;
        for (int i = 0; i < NUM_EVENTS; i++) {
            executor.scheduleHaxwellEvent(createHaxwellSubscription(i));
        }

        Thread.sleep(50); // Give us a little bit of time to ensure nothing cam through yet

        assertTrue(eventListener.receivedEvents.isEmpty());

        List<Future<?>> futures = executor.flush();
        assertFalse(futures.isEmpty());

        for (int retry = 0; retry < 50; retry++) {
            if (eventListener.receivedEvents.size() >= NUM_EVENTS) {
                break;
            }
            Thread.sleep(250);
        }

        assertEquals(NUM_EVENTS, eventListener.receivedEvents.size());
    }

    @Test
    public void testScheduleHaxwellSubscription_EventOverflow() throws InterruptedException {
        DelayingEventListener eventListener = new DelayingEventListener();
        HaxwellEventExecutor executor = new HaxwellEventExecutor(eventListener, getExecutors(1), 1, haxwellMetrics);
        final int NUM_EVENTS = 50;
        for (int i = 0; i < NUM_EVENTS; i++) {
            executor.scheduleHaxwellEvent(createHaxwellSubscription(i));
        }
        List<Future<?>> futures = executor.flush();

        // We're running with a single thread and no batching, so there should be just as many
        // futures as there are events
        assertEquals(NUM_EVENTS, futures.size());

        Thread.sleep(500);

        for (int retry = 0; retry < 50; retry++) {
            if (eventListener.receivedEvents.size() >= NUM_EVENTS) {
                break;
            }
            Thread.sleep(250);
        }

        assertEquals(NUM_EVENTS, eventListener.receivedEvents.size());

    }

    static class RecordingEventListener implements HaxwellEventListener {

        List<HaxwellRow> receivedEvents = Lists.newArrayList();

        @Override
        public synchronized void processEvents(List<HaxwellRow> events) {
            receivedEvents.addAll(events);
        }

    }

    static class DelayingEventListener implements HaxwellEventListener {

        List<HaxwellRow> receivedEvents = Collections.synchronizedList(Lists.newArrayList());

        @Override
        public void processEvents(List<HaxwellRow> events) {
            for (HaxwellRow event : events) {
                try {
                    Thread.sleep(250);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException(e);
                }
                receivedEvents.add(event);
            }
        }

    }
}
