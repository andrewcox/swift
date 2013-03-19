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
package com.facebook.swift.generator;

import com.google.common.collect.Lists;
import com.google.common.io.Files;
import com.google.common.io.Resources;
import org.testng.annotations.Test;

import java.io.File;

/**
 * Parses a Thrift IDL file and writes out initial annotated java classes.
 */
public class TestSwiftGenerator
{
    private static final String OUTPUT_FOLDER = Files.createTempDir().getPath();

    @Test
    public void testGenerator() throws Exception
    {
        final SwiftGeneratorConfig config = SwiftGeneratorConfig.builder()
                        .inputBase(Resources.getResource(TestSwiftGenerator.class, "/").toURI())
                        .outputFolder(Files.createTempDir())
                        .generateIncludedCode(true)
                        .codeFlavor("java-immutable")
                        .addTweak(SwiftGeneratorTweak.ADD_CLOSEABLE_INTERFACE)
                        .addTweak(SwiftGeneratorTweak.EXTEND_RUNTIME_EXCEPTION)
                        .addTweak(SwiftGeneratorTweak.ADD_THRIFT_EXCEPTION)
                        .build();

        final SwiftGenerator generator = new SwiftGenerator(config);
        generator.parse(
                Lists.newArrayList(
                        Resources.getResource(TestSwiftGenerator.class, "/hive/metastore.thrift").toURI(),
                        Resources.getResource(TestSwiftGenerator.class, "/Maestro.thrift").toURI(),
                        Resources.getResource(TestSwiftGenerator.class, "/SameFileInheritance.thrift").toURI()
                )
        );
    }
}
