package com.facebook.swift.codec;

import com.facebook.swift.codec.metadata.ThriftType;
import com.google.common.reflect.TypeToken;
import org.apache.thrift.protocol.TProtocol;

import java.lang.reflect.Type;

public class FutureCodecDelegate<T> implements ThriftCodec<T>
{
    private final ThriftCodecManager codecManager;
    private final TypeToken<T> typeToken;

    public FutureCodecDelegate(ThriftCodecManager codecManager, Type javaType)
    {
        this.codecManager = codecManager;
        this.typeToken = (TypeToken<T>) TypeToken.of(javaType);
    }

    @Override
    public ThriftType getType()
    {
        return codecManager.getCodec(typeToken).getType();
    }

    @Override
    public T read(TProtocol protocol) throws Exception
    {
        return codecManager.getCodec(typeToken).read(protocol);
    }

    @Override
    public void write(T value, TProtocol protocol) throws Exception
    {
        codecManager.getCodec(typeToken).write(value, protocol);
    }
}
