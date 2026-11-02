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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

import org.apache.cxf.Bus;
import org.apache.cxf.BusFactory;
import org.apache.cxf.bus.extension.ExtensionManagerBus;

import org.junit.Test;

public class CachedOutputStreamTest extends CachedStreamTestBase {
    private static final String TEST_DATA = "Hello World! The content is larger than the threshold.";
    
    @Override
    protected void reloadDefaultProperties() {
        CachedOutputStream.setDefaultThreshold(-1);
        CachedOutputStream.setDefaultMaxSize(-1);
        CachedOutputStream.setDefaultCipherTransformation(null);
    }
    
    @Override
    protected Object createCache() {
        return new CachedOutputStream();
    }
    
    @Override
    protected Object createCache(long threshold) {
        return createCache(threshold, null);
    }
    
    @Override
    protected Object createCache(long threshold, String transformation) {
        CachedOutputStream cos = new CachedOutputStream();
        cos.setThreshold(threshold);
        cos.setCipherTransformation(transformation);
        return cos;
    }
    
    @Override
    protected String getResetOutValue(String result, Object cache) throws IOException {
        CachedOutputStream cos = (CachedOutputStream)cache;
        cos.write(result.getBytes());
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        cos.resetOut(out, true);
        return out.toString();        
    }
    
    @Override
    protected File getTmpFile(String result, Object cache) throws IOException {
        CachedOutputStream cos = (CachedOutputStream)cache;
        cos.write(result.getBytes("utf-8"));
        cos.flush();
        return cos.getTempFile();
    }

    @Override
    protected Object getInputStreamObject(Object cache) throws IOException {
        return ((CachedOutputStream)cache).getInputStream();
    }
    
    @Override
    protected String readFromStreamObject(Object obj) throws IOException {
        return readFromStream((InputStream)obj);
    }

    @Test
    public void testUnclosedStreamTempFileIsCleaned() throws Exception {
        final Bus oldbus = BusFactory.getThreadDefaultBus(false);
        final Bus bus = new ExtensionManagerBus();
        try {
            BusFactory.setThreadDefaultBus(bus);
            final DelayedCachedOutputStreamCleaner cleaner = 
                (DelayedCachedOutputStreamCleaner)bus.getExtension(CachedOutputStreamCleaner.class);
            assertNotNull(cleaner);
            
            final CachedOutputStream cos = new CachedOutputStream(16);
            final File tmpfile = getTmpFile(TEST_DATA, cos);
            assertNotNull("expects a tmp file", tmpfile);
            assertTrue("expects a tmp file", tmpfile.exists());
            assertEquals(1, cleaner.size());
            
            // The stream is never closed (leaked), the cleaner has to close it and delete the tmp file
            cleaner.forceClean();
            assertFalse("expects the tmp file to be deleted", tmpfile.exists());
            assertEquals(0, cleaner.size());
        } finally {
            BusFactory.setThreadDefaultBus(oldbus);
            bus.shutdown(true);
        }
    }

    @Test
    public void testUnclosedInputStreamTempFileIsCleaned() throws Exception {
        final Bus oldbus = BusFactory.getThreadDefaultBus(false);
        final Bus bus = new ExtensionManagerBus();
        try {
            BusFactory.setThreadDefaultBus(bus);
            final DelayedCachedOutputStreamCleaner cleaner = 
                (DelayedCachedOutputStreamCleaner)bus.getExtension(CachedOutputStreamCleaner.class);
            
            final CachedOutputStream cos = new CachedOutputStream(16);
            final File tmpfile = getTmpFile(TEST_DATA, cos);
            assertNotNull("expects a tmp file", tmpfile);
            
            // The input stream is never closed (leaked) so closing the output stream keeps the tmp file
            final InputStream in = cos.getInputStream();
            assertNotNull(in);
            cos.close();
            assertTrue("expects a tmp file", tmpfile.exists());
            assertEquals(2, cleaner.size());
            
            cleaner.forceClean();
            assertFalse("expects the tmp file to be deleted", tmpfile.exists());
            assertEquals(0, cleaner.size());
        } finally {
            BusFactory.setThreadDefaultBus(oldbus);
            bus.shutdown(true);
        }
    }

    @Test
    public void testUnclosedStreamTempFileIsCleanedOnSchedule() throws Exception {
        final Bus oldbus = BusFactory.getThreadDefaultBus(false);
        final Bus bus = new ExtensionManagerBus();
        bus.setProperty(CachedConstants.CLEANER_DELAY_BUS_PROP, 2500); /* 2.5 seconds */
        try {
            BusFactory.setThreadDefaultBus(bus);
            final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
            
            final CachedOutputStream cos = new CachedOutputStream(16);
            final File tmpfile = getTmpFile(TEST_DATA, cos);
            assertNotNull("expects a tmp file", tmpfile);
            assertTrue("expects a tmp file", tmpfile.exists());
            
            // The stream is never closed (leaked), the tmp file has to be deleted on schedule
            final long deadline = System.currentTimeMillis() + 7000;
            while (tmpfile.exists() && System.currentTimeMillis() < deadline) {
                Thread.sleep(25);
            }
            assertFalse("expects the tmp file to be deleted", tmpfile.exists());
            assertEquals(0, cleaner.size());
        } finally {
            BusFactory.setThreadDefaultBus(oldbus);
            bus.shutdown(true);
        }
    }

    @Test
    public void testClosedStreamsAreUnregistered() throws Exception {
        final Bus oldbus = BusFactory.getThreadDefaultBus(false);
        final Bus bus = new ExtensionManagerBus();
        try {
            BusFactory.setThreadDefaultBus(bus);
            final CachedOutputStreamCleaner cleaner = bus.getExtension(CachedOutputStreamCleaner.class);
            
            final CachedOutputStream cos = new CachedOutputStream(16);
            final File tmpfile = getTmpFile(TEST_DATA, cos);
            assertNotNull("expects a tmp file", tmpfile);
            final InputStream in = cos.getInputStream();
            assertEquals(2, cleaner.size());
            
            cos.close();
            in.close();
            assertFalse("expects the tmp file to be deleted", tmpfile.exists());
            assertEquals(0, cleaner.size());
            
            // Not backed by the file anymore, should be unregistered
            final CachedOutputStream cos2 = new CachedOutputStream(16);
            final File tmpfile2 = getTmpFile(TEST_DATA, cos2);
            assertNotNull("expects a tmp file", tmpfile2);
            assertEquals(1, cleaner.size());
            final ByteArrayOutputStream out = new ByteArrayOutputStream();
            cos2.resetOut(out, true);
            assertFalse("expects the tmp file to be deleted", tmpfile2.exists());
            assertEquals(0, cleaner.size());
        } finally {
            BusFactory.setThreadDefaultBus(oldbus);
            bus.shutdown(true);
        }
    }
}
    
   
