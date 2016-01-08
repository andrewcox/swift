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

import java.lang.reflect.Type;
import java.util.Objects;

public class RecursiveThriftTypeHolder implements ThriftTypeHolder
{
    private final ThriftCatalog catalog;
    private final Type javaType;

    public RecursiveThriftTypeHolder(ThriftCatalog catalog, Type javaType)
    {
        this.catalog = catalog;
        this.javaType = javaType;
    }

    @Override
    public boolean isRecursive()
    {
        return true;
    }

    @Override
    public ThriftType resolve()
    {
        return catalog.getCachedThriftType(javaType);
    }

    @Override
    public String toString()
    {
        return "UnresolvedThriftTYpeHolder for " + javaType;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(catalog, javaType);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || obj.getClass() != RecursiveThriftTypeHolder.class) {
            return false;
        }

        RecursiveThriftTypeHolder that = (RecursiveThriftTypeHolder) obj;

        return Objects.equals(this.catalog, that.catalog) &&
               Objects.equals(this.javaType, that.javaType);
    }                                         ;
}
