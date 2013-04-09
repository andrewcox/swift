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

import com.facebook.nifty.core.NettyConfigBuilder;
import com.facebook.nifty.core.NiftyBootstrap;
import com.facebook.nifty.core.ThriftServerDef;
import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import io.netty.util.Timer;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import io.netty.channel.group.DefaultChannelGroup;
import io.netty.util.HashedWheelTimer;

import org.weakref.jmx.Managed;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.concurrent.ExecutorService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import static com.facebook.nifty.core.ShutdownUtil.shutdownExecutor;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ThriftServer implements Closeable
{
    private enum State {
        NOT_STARTED,
        RUNNING,
        CLOSED,
    }

    private final NiftyBootstrap bootstrap;
    private final int workerThreads;
    private final int port;
    private final DefaultChannelGroup allChannels = new DefaultChannelGroup();

    private final ExecutorService workerExecutor;

    private State state = State.NOT_STARTED;

    public ThriftServer(TProcessor processor)
    {
        this(processor, new ThriftServerConfig());
    }

    public ThriftServer(TProcessor processor, ThriftServerConfig config)
    {
        this(processor, config, new HashedWheelTimer());
    }

    @Inject
    public ThriftServer(TProcessor processor, ThriftServerConfig config, @ThriftServerTimer Timer timer)
    {
        TProcessorFactory processorFactory = new TProcessorFactory(processor);

        port = getSpecifiedOrRandomPort(config);

        workerThreads = config.getWorkerThreads();

        workerExecutor = newFixedThreadPool(workerThreads, new ThreadFactoryBuilder().setNameFormat("thrift-worker-%s").build());

        ThriftServerDef thriftServerDef = ThriftServerDef.newBuilder()
                                                         .name("thrift")
                                                         .listen(port)
                                                         .limitFrameSizeTo((int) config.getMaxFrameSize().toBytes())
                                                         .clientIdleTimeout(config.getClientIdleTimeout())
                                                         .withProcessorFactory(processorFactory)
                                                         .using(workerExecutor).build();

        bootstrap = new NiftyBootstrap(ImmutableSet.of(thriftServerDef), new NettyConfigBuilder(), allChannels);
    }

    private int getSpecifiedOrRandomPort(ThriftServerConfig config)
    {
        if (config.getPort() != 0) {
            return config.getPort();
        }
        try (ServerSocket s = new ServerSocket()) {
            s.bind(new InetSocketAddress(0));
            return s.getLocalPort();
        }
        catch (IOException e) {
            throw new IllegalStateException("Unable to get a random port", e);
        }
    }

    @Managed
    public int getPort()
    {
        return port;
    }

    @Managed
    public int getWorkerThreads()
    {
        return workerThreads;
    }

    public synchronized boolean isRunning() {
        return state == State.RUNNING;
    }

    @PostConstruct
    public synchronized ThriftServer start()
            throws InterruptedException
    {
        Preconditions.checkState(state != State.CLOSED, "Thrift server is closed");
        if (state == State.NOT_STARTED) {
            bootstrap.start();
            state = State.RUNNING;
        }
        return this;
    }

    @PreDestroy
    @Override
    public synchronized void close()
    {
        if (state == State.CLOSED) {
            return;
        }

        if (state == State.RUNNING) {
            try {
                bootstrap.stop();

                shutdownExecutor(workerExecutor, "workerExecutor");
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        state = State.CLOSED;
    }
}
