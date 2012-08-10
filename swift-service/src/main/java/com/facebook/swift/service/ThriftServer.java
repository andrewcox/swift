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
import com.facebook.nifty.core.NettyServerTransport;
import com.facebook.nifty.core.ThriftServerDef;
import com.facebook.nifty.core.ThriftServerDefBuilder;
import com.google.common.base.Preconditions;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.inject.Inject;
import org.apache.thrift.TProcessor;
import org.apache.thrift.TProcessorFactory;
import org.jboss.netty.channel.ServerChannelFactory;
import org.jboss.netty.channel.group.DefaultChannelGroup;
import org.jboss.netty.channel.socket.nio.NioServerSocketChannelFactory;
import org.jboss.netty.util.HashedWheelTimer;
import org.jboss.netty.util.Timer;
import org.weakref.jmx.Managed;

import java.io.Closeable;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import static com.facebook.nifty.core.ShutdownUtil.shutdownChannelFactory;
import static com.facebook.nifty.core.ShutdownUtil.shutdownExecutor;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;

public class ThriftServer implements Closeable
{
    private enum State
    {
        NOT_STARTED,
        RUNNING,
        CLOSED,
    }

    private final NettyServerTransport transport;
    private final int workerThreads;
    private final int configuredPort;
    private final DefaultChannelGroup allChannels = new DefaultChannelGroup();

    private final ExecutorService acceptorExecutor;
    private final ExecutorService ioExecutor;
    private final ExecutorService workerExecutor;

    private final ServerChannelFactory serverChannelFactory;

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

        configuredPort = config.getPort();

        workerThreads = config.getWorkerThreads();

        acceptorExecutor = newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("thrift-acceptor-%s").build());
        ioExecutor = newCachedThreadPool(new ThreadFactoryBuilder().setNameFormat("thrift-io-%s").build());

        serverChannelFactory = new NioServerSocketChannelFactory(acceptorExecutor, ioExecutor);

        NettyConfigBuilder nettyConfigBuilder = new NettyConfigBuilder();
        nettyConfigBuilder.getServerSocketChannelConfig().setBacklog(1024);

        ThriftServerDefBuilder thriftServerDefBuilder =
                ThriftServerDef.newBuilder()
                               .name("thrift")
                               .listen(configuredPort)
                               .limitFrameSizeTo((int) config.getMaxFrameSize().toBytes())
                               .clientIdleTimeout(config.getClientIdleTimeout())
                               .withProcessorFactory(processorFactory);

        if (getWorkerThreads() <= 0) {
            workerExecutor = null;
            thriftServerDefBuilder.using(new Executor()
            {
                @Override
                public void execute(Runnable command)
                {
                    command.run();
                }
            });
        } else {
            workerExecutor = newFixedThreadPool(getWorkerThreads(),
                                                new ThreadFactoryBuilder().setNameFormat("thrift-worker-%s").build());
            thriftServerDefBuilder.using(workerExecutor);
        }

        transport = new NettyServerTransport(thriftServerDefBuilder.build(), nettyConfigBuilder, allChannels, timer);
    }

    @Managed
    public int getPort()
    {
        if (configuredPort != 0) {
            return configuredPort;
        }

        if (transport.getServerChannel() == null) {
            throw new IllegalStateException("Cannot determine the randomly port before the server is started");
        }

        return getBoundPort();
    }

    private int getBoundPort()
    {
        // If the server was configured to bind to port 0, a random port will actually be bound instead
        SocketAddress socketAddress = transport.getServerChannel().getLocalAddress();
        if (socketAddress instanceof InetSocketAddress) {
            InetSocketAddress inetSocketAddress = (InetSocketAddress) socketAddress;
            return inetSocketAddress.getPort();
        }

        throw new IllegalStateException("Unable to determine the bound port");
    }

    @Managed
    public int getWorkerThreads()
    {
        return workerThreads;
    }

    public synchronized boolean isRunning()
    {
        return state == State.RUNNING;
    }

    @PostConstruct
    public synchronized ThriftServer start()
    {
        Preconditions.checkState(state != State.CLOSED, "Thrift server is closed");
        if (state == State.NOT_STARTED) {
            transport.start(serverChannelFactory);
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
                transport.stop();

                if (workerExecutor != null) {
                    shutdownExecutor(workerExecutor, "workerExecutor");
                }
                shutdownChannelFactory(serverChannelFactory, acceptorExecutor, ioExecutor, allChannels);
            }
            catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }

        state = State.CLOSED;
    }
}
