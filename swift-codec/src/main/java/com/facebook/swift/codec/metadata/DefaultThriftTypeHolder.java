/*
 * Copyright (C) 2012 ${project.organization.name}
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
package com.facebook.swift.codec.metadata;

import java.util.Objects;

public class DefaultThriftTypeHolder implements ThriftTypeHolder
{
    private final ThriftType thriftType;

    public DefaultThriftTypeHolder(ThriftType thriftType)
    {
        this.thriftType = thriftType;
    }

    @Override
    public boolean isRecursive()
    {
        return false;
    }

    @Override
    public ThriftType resolve()
    {
        return thriftType;
    }

    @Override
    public String toString()
    {
        return "ResolvedThriftTYpeHolder for " + thriftType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(thriftType.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }

        if (obj == null || obj.getClass() != DefaultThriftTypeHolder.class) {
            return false;
        }

        DefaultThriftTypeHolder that = (DefaultThriftTypeHolder) obj;

        return Objects.equals(this.thriftType, that.thriftType);
    }
}
