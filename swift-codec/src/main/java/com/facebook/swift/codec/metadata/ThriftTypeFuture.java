/**
 * Copyright 2016 Facebook, Inc.
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License. You may obtain
 * a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package com.facebook.swift.codec.metadata;

import java.lang.reflect.Type;
import java.util.Objects;

public class ThriftTypeFuture
{
    private ThriftType thriftType;
    private final Type javaType;
    private final ThriftCatalog catalog;

    public ThriftTypeFuture(ThriftType thriftType)
    {
        synchronized (this) {
            this.thriftType = thriftType;
            this.catalog = null;
            this.javaType = null;
        }
    }

    public ThriftTypeFuture(Type javaType, ThriftCatalog catalog)
    {
        synchronized (this) {
            this.thriftType = null;
            this.javaType = javaType;
            this.catalog = catalog;
        }
    }

    public ThriftType get()
    {
        return thriftType;
    }

    public boolean isResolved()
    {
        synchronized (this) {
            return thriftType != null;
        }
    }

    public ThriftType resolve()
    {
        synchronized (this) {
            if (isResolved()) {
                return thriftType;
            }
            else {
                thriftType = catalog.getThriftType(javaType);
                return thriftType;
            }
        }
    }

    @Override
    public String toString()
    {
        // TODO: dump something like "unresolved ThriftType for recursive struct <javaType>" if
        // ThriftType hasn't been resolved, else defer to ThriftType
        return super.toString();
    }

    @Override
    public int hashCode()
    {
        if (thriftType != null) {
            return thriftType.hashCode();
        }
        else
        {
            return Objects.hash(catalog, javaType);
        }
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != ThriftTypeFuture.class) {
            return false;
        }

        ThriftTypeFuture that = (ThriftTypeFuture) obj;

        if (thriftType == null) {
            if (that.thriftType != null) {
                return this.javaType == that.thriftType.getJavaType();
            }
            else {
                return this.javaType == that.javaType;
            }
        }
        else /* (thriftType != null) */ {
            if (that.thriftType != null) {
                return this.thriftType == that.thriftType;
            }
            else {
                return this.thriftType.getJavaType() == that.javaType;
            }
        }
    }
}
