/**
 * Copyright 2012 Facebook, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.facebook.swift.loadtest;

import org.apache.thrift.TException;

import com.facebook.swift.service.ThriftException;
import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

import java.nio.ByteBuffer;

@ThriftService
public class LoadTestHandler
{
    // Methods for testing server performance
    // Fairly simple requests, to minimize serialization overhead

    /**
     * noop() returns immediately, to test behavior of fast, cheap operations
     */
    @ThriftMethod
    public void noop() {}
    @ThriftMethod(oneway = true)
    public void onewayNoop() {}

    /**
     * asyncNoop() is like noop() except for one minor difference in the async
     * implementation.
     *
     * In the async handler, noop() invokes the callback immediately, while
     * asyncNoop() uses runInLoop() to invoke the callback.
     */
    @ThriftMethod
    void asyncNoop() {}

    /**
     * sleep() waits for the specified time period before returning,
     * to test slow, but not CPU-intensive operations
     */
    @ThriftMethod
    void sleep(long microseconds) {
        try {
            long ms = microseconds / 1000;
            int us = (int) (microseconds % 1000);
            Thread.sleep(ms, us);
        }
        catch (InterruptedException e) {
        }
    }

    @ThriftMethod(oneway = true)
    void onewaySleep(long microseconds) {
        sleep(microseconds);
    }

    /**
     * burn() uses as much CPU as possible for the desired time period,
     * to test CPU-intensive operations
     */
    @ThriftMethod
    void burn(long microseconds) {
        burnImpl(microseconds);
    }

    @ThriftMethod(oneway = true)
    void onewayBurn(long microseconds) {
        burnImpl(microseconds);
    }

    /**
     * badSleep() is like sleep(), except that it exhibits bad behavior in the
     * async implementation.
     *
     * Instead of returning control to the event loop, it actually sleeps in the
     * worker thread for the specified duration.  This tests how well the thrift
     * infrastructure responds to badly written handler code.
     */
    @ThriftMethod
    void badSleep(long microseconds) {
        burnImpl(microseconds);
    }

    /**
     * badBurn() is like burn(), except that it exhibits bad behavior in the
     * async implementation.
     *
     * The normal burn() call periodically yields to the event loop, while
     * badBurn() does not.  This tests how well the thrift infrastructure
     * responds to badly written handler code.
     */
    @ThriftMethod
    public void badBurn(long microseconds) {
        burnImpl(microseconds);
    }

    private void burnImpl(long microseconds) {
        long end = System.nanoTime() + microseconds;
        while (System.nanoTime() < end) {}
    }

    /**
     * throw an error
     */
    @ThriftMethod(
            exception = { @ThriftException(type = LoadError.class, id = 1) }
    )
    public void throwError(int code)
            throws LoadError {
        throw new LoadError(code);
    }

    /**
     * throw an unexpected error (not declared in the .thrift file)
     */
    @ThriftMethod
    void throwUnexpected(int code)
            throws LoadError {
        throw new LoadError(code);
    }

    /**
     * throw an error in a oneway call,
     * just to make sure the internal thrift code handles it properly
     */
    @ThriftMethod(
            oneway = true,
            exception = { @ThriftException(type = LoadError.class, id = 1) }
    )
    void onewayThrow(int code)
            throws LoadError {
        throw new LoadError(code);
    }

    /**
     * Send some data to the server.
     *
     * The data is ignored.  This is primarily to test the server I/O
     * performance.
     */
    @ThriftMethod
    void send(ByteBuffer data) {}

    /**
     * Send some data to the server.
     *
     * The data is ignored.  This is primarily to test the server I/O
     * performance.
     */
    @ThriftMethod(oneway = true)
    void onewaySend(ByteBuffer data) {}

    /**
     * Receive some data from the server.
     *
     * The contents of the data are undefined.  This is primarily to test the
     * server I/O performance.
     */
    @ThriftMethod
    ByteBuffer recv(long recvBytes) {
        return ByteBuffer.allocate((int) recvBytes);
    }

    /**
     * Send and receive data
     */
    @ThriftMethod
    ByteBuffer sendrecv(ByteBuffer data, long recvBytes) {
        return ByteBuffer.allocate((int) recvBytes);
    }

    /**
     * Echo data back to the client.
     */
    @ThriftMethod
    ByteBuffer echo(ByteBuffer data) {
        return data;
    }

    /**
     * Add the two integers
     */
    @ThriftMethod
    long add(long a, long b) {
        return a + b;
    }
}
