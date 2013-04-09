/*
 * Copyright (C) 2012 Facebook, Inc.
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
package com.facebook.swift.service;

import com.facebook.nifty.client.NiftyClientChannel;
import com.facebook.nifty.core.ThriftMessage;
import org.apache.thrift.TException;
import org.apache.thrift.transport.TTransportException;
import io.netty.buffer.ByteBuf;

import java.util.concurrent.CountDownLatch;

/**
 * Helper class simulating synchronous operations on asynchronous {@link NiftyClientChannel}
 */
class SyncClientHelpers
{
    /**
     * Sends a single message synchronously, and blocks until the responses is received.
     * <p/>
     * NOTE: the underlying transport may be non-blocking, in which case the blocking is simulated
     * by waits instead of using blocking network operations.
     *
     * @param buffer
     * @return The response, stored in a ByteBuf
     * @throws TException           if an error occurs while serializing or sending the request or
     *                              while receiving or de-serializing the response
     * @throws InterruptedException if the operation is interrupted before the response arrives
     */
    public static ThriftMessage sendSynchronousTwoWayMessage(NiftyClientChannel channel, final ByteBuf buffer)
            throws TException, InterruptedException
    {
        final ByteBuf[] responseHolder = new ByteBuf[1];
        final TException[] exceptionHolder = new TTransportException[1];
        final CountDownLatch latch = new CountDownLatch(1);

        responseHolder[0] = null;
        exceptionHolder[0] = null;

        ThriftMessage request = new ThriftMessage(buffer, channel.getTransportType());
        channel.sendAsynchronousRequest(request, false, new NiftyClientChannel.Listener()
        {
            @Override
            public void onRequestSent()
            {
            }

            @Override
            public void onResponseReceived(ThriftMessage response)
            {
                responseHolder[0] = response.getBuffer().retain();
                latch.countDown();
            }

            @Override
            public void onChannelError(TException e)
            {
                exceptionHolder[0] = e;
                latch.countDown();
            }
        });

        latch.await();

        if (exceptionHolder[0] != null) {
            throw exceptionHolder[0];
        }

        return new ThriftMessage(responseHolder[0], channel.getTransportType());
    }

    /**
     * Sends a single message synchronously, blocking until the send is complete. Does not wait for
     * a response.
     * <p/>
     * NOTE: the underlying transport may be non-blocking, in which case the blocking is simulated
     * by waits instead of using blocking network operations.
     *
     * @param buffer
     * @throws TException           if a network or protocol error occurs while serializing or
     *                              sending the request
     * @throws InterruptedException if the thread is interrupted before the request is sent
     */
    public static void sendSynchronousOneWayMessage(NiftyClientChannel channel, final ByteBuf buffer)
            throws TException, InterruptedException
    {

        final TException[] exceptionHolder = new TTransportException[1];
        final CountDownLatch latch = new CountDownLatch(1);

        exceptionHolder[0] = null;

        ThriftMessage request = new ThriftMessage(buffer, channel.getTransportType());
        channel.sendAsynchronousRequest(request, true, new NiftyClientChannel.Listener()
        {
            @Override
            public void onRequestSent()
            {
                latch.countDown();
            }

            @Override
            public void onResponseReceived(ThriftMessage response)
            {
            }

            @Override
            public void onChannelError(TException e)
            {
                exceptionHolder[0] = e;
                latch.countDown();
            }
        });

        latch.await();

        if (exceptionHolder[0] != null) {
            throw exceptionHolder[0];
        }
    }
}
