/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.cxf.io;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cxf.Bus;
import org.apache.cxf.bus.extension.ExtensionManagerBus;

import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

public class DelayedCachedOutputStreamCleanerTest extends Assert {
    private Bus bus;

    @After
    public void tearDown() {
        if (bus != null) {
            bus.shutdown(true);
            bus = null;
        }
    }

    @Test
    public void testNoop() {
        bus = createBus(0);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertTrue(cleaner instanceof DelayedCachedOutputStreamCleaner); /* noop */

        assertNoopCleaner(cleaner);
    }

    @Test
    public void testForceClean() throws InterruptedException {
        bus = new ExtensionManagerBus();

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertTrue(cleaner instanceof DelayedCachedOutputStreamCleaner);

        final AtomicBoolean latch = new AtomicBoolean(false);
        final Closeable closeable = new BooleanCloseable(latch);
        cleaner.register(closeable);

        final DelayedCachedOutputStreamCleaner delayedCleaner = (DelayedCachedOutputStreamCleaner) cleaner;
        delayedCleaner.forceClean();

        // Await for Closeable::close to be called
        assertTrue(latch.get());
    }

    @Test
    public void testClean() throws InterruptedException {
        final AtomicInteger latch = new AtomicInteger();
        final Closeable closeable1 = new CountingCloseable(latch);
        final Closeable closeable2 = new CountingCloseable(latch);

        /* Delay of 2.5 seconds */
        bus = createBus(2500);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        cleaner.register(closeable1);
        cleaner.register(closeable2);

        // Await for Closeable::close to be called on schedule
        awaitAtMost(latch, 2, 5000);
        assertTrue(cleaner instanceof DelayedCachedOutputStreamCleaner);
    }

    @Test
    public void testForceCleanForEmpty() throws InterruptedException {
        bus = new ExtensionManagerBus();

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertTrue(cleaner instanceof DelayedCachedOutputStreamCleaner);

        final AtomicBoolean latch = new AtomicBoolean(false);
        final Closeable closeable = new BooleanCloseable(latch);

        cleaner.register(closeable);
        cleaner.unregister(closeable);

        final DelayedCachedOutputStreamCleaner delayedCleaner = (DelayedCachedOutputStreamCleaner) cleaner;
        delayedCleaner.forceClean();

        // Closeable::close should not be called
        assertFalse(latch.get());
    }

    @Test
    public void testForceCleanException() throws InterruptedException {
        bus = new ExtensionManagerBus();

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertTrue(cleaner instanceof DelayedCachedOutputStreamCleaner);

        final AtomicInteger latch = new AtomicInteger();
        final Closeable closeable2 = new CountingCloseable(latch);
        final Closeable closeable1 = new Closeable() {
            public void close() throws IOException {
                latch.incrementAndGet();
                throw new IOException("Simulated");
            }
        };
        cleaner.register(closeable1);
        cleaner.register(closeable2);

        final DelayedCachedOutputStreamCleaner delayedCleaner = (DelayedCachedOutputStreamCleaner) cleaner;
        delayedCleaner.forceClean();

        // Try to call force clean one more time
        delayedCleaner.forceClean();

        // Await for Closeable::close to be called
        assertEquals(2, latch.get());
    }

    @Test
    public void testCleanOnShutdown() throws InterruptedException {
        /* Delay of 2.5 seconds */
        bus = createBus(2500);

        final AtomicBoolean latch = new AtomicBoolean();
        final Closeable closeable = new BooleanCloseable(latch);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        cleaner.register(closeable);

        // Closes the bus, the cleaner should cancel the internal timer(s)
        bus.shutdown(true);

        // The Closeable::close should be called on shutdown
        assertTrue(latch.get());
    }

    @Test
    public void testCleanOnShutdownDisabled() throws InterruptedException {
        /* Delay of 2.5 seconds */
        bus = createBus(2500);
        bus.setProperty(CachedConstants.CLEANER_CLEAN_ON_SHUTDOWN_BUS_PROP, false);

        final AtomicBoolean latch = new AtomicBoolean();
        final Closeable closeable = new BooleanCloseable(latch);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        cleaner.register(closeable);

        // Closes the bus, the cleaner should cancel the internal timer(s)
        bus.shutdown(true);

        // The Closeable::close should not be called since timer(s) is cancelled
        assertStaysFalse(latch, 3000);
    }

    @Test
    public void testNegativeDelay() throws InterruptedException {
        bus = createBus(-1);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertTrue(cleaner instanceof DelayedCachedOutputStreamCleaner); /* noop */

        assertNoopCleaner(cleaner);
    }

    @Test
    public void testTooSmallDelay() throws InterruptedException {
        bus = createBus(1500);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertTrue(cleaner instanceof DelayedCachedOutputStreamCleaner); /* noop */

        assertNoopCleaner(cleaner);
    }

    @Test
    public void testDelayedClean() throws InterruptedException {
        final AtomicInteger latch = new AtomicInteger();
        final Closeable closeable1 = new CountingCloseable(latch);
        final Closeable closeable2 = new CountingCloseable(latch);

        /* Delay of 2.5 seconds */
        bus = createBus(2500);

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        cleaner.register(closeable1);

        Thread.sleep(2000);
        cleaner.register(closeable2);

        // Await for Closeable::close to be called on schedule
        awaitAtMost(latch, 1, 3000);

        // Await for Closeable::close to be called on schedule
        awaitAtMost(latch, 2, 5000);

        assertTrue(cleaner instanceof DelayedCachedOutputStreamCleaner);
    }

    @Test
    public void testCleanDoesNotCloseStreamsBeforeDelayExpires() throws InterruptedException {
        // The delay is expressed in milliseconds: a registered (live) stream must not be
        // considered as expired (and closed) before the delay elapses.
        bus = new ExtensionManagerBus(); /* default delay of 30 minutes */

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertTrue(cleaner instanceof DelayedCachedOutputStreamCleaner);

        final AtomicBoolean latch = new AtomicBoolean(false);
        final Closeable closeable = new BooleanCloseable(latch);
        cleaner.register(closeable);

        Thread.sleep(100);
        cleaner.clean();

        // Not expired yet, Closeable::close should not be called
        assertFalse(latch.get());
        assertEquals(1, cleaner.size());

        cleaner.unregister(closeable);
        assertEquals(0, cleaner.size());
    }

    @Test
    public void testSize() {
        bus = new ExtensionManagerBus();

        final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
        assertEquals(0, cleaner.size());

        final Closeable closeable1 = new CountingCloseable(new AtomicInteger());
        final Closeable closeable2 = new CountingCloseable(new AtomicInteger());
        cleaner.register(closeable1);
        cleaner.register(closeable2);
        assertEquals(2, cleaner.size());

        cleaner.unregister(closeable1);
        assertEquals(1, cleaner.size());

        ((DelayedCachedOutputStreamCleaner) cleaner).forceClean();
        assertEquals(0, cleaner.size());
    }

    private static Bus createBus(final Object delay) {
        final Bus b = new ExtensionManagerBus();
        b.setProperty(CachedConstants.CLEANER_DELAY_BUS_PROP, delay);
        return b;
    }

    private void assertNoopCleaner(final CachedOutputStreamCleaner cleaner) {
        final AtomicBoolean latch = new AtomicBoolean(false);
        final Closeable closeable = new BooleanCloseable(latch);
        cleaner.register(closeable);

        final DelayedCachedOutputStreamCleaner delayedCleaner = (DelayedCachedOutputStreamCleaner) cleaner;
        delayedCleaner.forceClean();

        // Noop, Closeable::close should not be called
        assertFalse(latch.get());
        assertEquals(0, cleaner.size());
    }

    private static void awaitAtMost(final AtomicInteger latch, final int expected, final long timeoutMs)
        throws InterruptedException {
        final long deadline = System.currentTimeMillis() + timeoutMs;
        while (latch.get() != expected && System.currentTimeMillis() < deadline) {
            Thread.sleep(25);
        }
        assertEquals(expected, latch.get());
    }

    private static void assertStaysFalse(final AtomicBoolean latch, final long durationMs)
        throws InterruptedException {
        final long deadline = System.currentTimeMillis() + durationMs;
        while (System.currentTimeMillis() < deadline) {
            assertFalse(latch.get());
            Thread.sleep(25);
        }
        assertFalse(latch.get());
    }

    private static final class BooleanCloseable implements Closeable {
        private final AtomicBoolean latch;

        BooleanCloseable(final AtomicBoolean latch) {
            this.latch = latch;
        }

        public void close() {
            latch.compareAndSet(false, true);
        }
    }

    private static final class CountingCloseable implements Closeable {
        private final AtomicInteger latch;

        CountingCloseable(final AtomicInteger latch) {
            this.latch = latch;
        }

        public void close() {
            latch.incrementAndGet();
        }
    }
}
