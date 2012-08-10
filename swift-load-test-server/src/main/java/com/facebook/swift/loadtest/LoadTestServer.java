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

import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.codec.guice.ThriftCodecModule;
import com.facebook.swift.service.ThriftServer;
import com.facebook.swift.service.ThriftServerConfig;
import com.facebook.swift.service.ThriftServiceProcessor;
import com.facebook.swift.service.guice.ThriftServerModule;
import com.google.common.collect.ImmutableMap;
import com.google.inject.AbstractModule;
import com.google.inject.Guice;
import com.google.inject.Injector;
import io.airlift.configuration.ConfigurationFactory;
import io.airlift.configuration.ConfigurationModule;
import io.airlift.jmx.JmxModule;
import org.weakref.jmx.guice.MBeanModule;

import static com.facebook.swift.service.guice.ThriftServiceExporter.thriftServerBinder;

public class LoadTestServer {
    public static void main(String[] args) {
        Injector injector = Guice.createInjector(
                new ConfigurationModule(new ConfigurationFactory(ImmutableMap.of(
                        "thrift.port", "12345",
                        "thrift.threads.max", "24"
                ))),
                new ThriftCodecModule(),
                new ThriftServerModule(),
                new JmxModule(),
                new MBeanModule(),
                new AbstractModule()
                {
                    @Override
                    protected void configure()
                    {
                        bind(LoadTestHandler.class).toInstance(new LoadTestHandler());
                        thriftServerBinder(binder()).exportThriftService(LoadTestHandler.class);
                    }
                }
        );

        //ThriftCodecManager codecManager = new ThriftCodecManager();
        //LoadTestHandler handler = new LoadTestHandler();
        //ThriftServiceProcessor processor = new ThriftServiceProcessor(codecManager,
        //                                                              handler);
        //ThriftServerConfig config = new ThriftServerConfig();
        //config.setPort(12345);
        //config.setWorkerThreads(24);
        //
        //ThriftServer server = new ThriftServer(processor, config);
        //
        //server.start();

        injector.getInstance(ThriftServer.class).start();
    }
}
