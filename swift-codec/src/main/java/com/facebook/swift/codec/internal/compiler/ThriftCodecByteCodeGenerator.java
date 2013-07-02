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
package com.facebook.swift.codec.internal.compiler;

import com.facebook.swift.codec.ThriftCodec;
import com.facebook.swift.codec.ThriftCodecManager;
import com.facebook.swift.codec.ThriftProtocolType;
import com.facebook.swift.codec.internal.EnumThriftCodec;
import com.facebook.swift.codec.internal.TProtocolWriter;
import com.facebook.swift.codec.internal.builtin.ListThriftCodec;
import com.facebook.swift.codec.internal.builtin.MapThriftCodec;
import com.facebook.swift.codec.internal.builtin.SetThriftCodec;
import com.facebook.swift.codec.internal.builtin.StructThriftCodec;
import com.facebook.swift.codec.internal.compiler.byteCode.CaseStatement;
import com.facebook.swift.codec.internal.compiler.byteCode.ClassDefinition;
import com.facebook.swift.codec.internal.compiler.byteCode.FieldDefinition;
import com.facebook.swift.codec.internal.compiler.byteCode.LocalVariableDefinition;
import com.facebook.swift.codec.internal.compiler.byteCode.MethodDefinition;
import com.facebook.swift.codec.internal.compiler.byteCode.NamedParameterDefinition;
import com.facebook.swift.codec.internal.compiler.byteCode.ParameterizedType;
import com.facebook.swift.codec.metadata.ThriftConstructorInjection;
import com.facebook.swift.codec.metadata.ThriftExtraction;
import com.facebook.swift.codec.metadata.ThriftFieldExtractor;
import com.facebook.swift.codec.metadata.ThriftFieldInjection;
import com.facebook.swift.codec.metadata.ThriftFieldMetadata;
import com.facebook.swift.codec.metadata.ThriftInjection;
import com.facebook.swift.codec.metadata.ThriftMethodExtractor;
import com.facebook.swift.codec.metadata.ThriftMethodInjection;
import com.facebook.swift.codec.metadata.ThriftParameterInjection;
import com.facebook.swift.codec.metadata.ThriftStructMetadata;
import com.facebook.swift.codec.metadata.ThriftType;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.reflect.TypeToken;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolUtil;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.protocol.TType;
import org.objectweb.asm.ClassReader;
import org.objectweb.asm.ClassWriter;
import org.objectweb.asm.util.CheckClassAdapter;

import java.io.PrintWriter;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import javax.annotation.concurrent.NotThreadSafe;

import static com.facebook.swift.codec.ThriftProtocolType.BOOL;
import static com.facebook.swift.codec.ThriftProtocolType.BYTE;
import static com.facebook.swift.codec.ThriftProtocolType.DOUBLE;
import static com.facebook.swift.codec.ThriftProtocolType.ENUM;
import static com.facebook.swift.codec.ThriftProtocolType.I16;
import static com.facebook.swift.codec.ThriftProtocolType.I32;
import static com.facebook.swift.codec.ThriftProtocolType.I64;
import static com.facebook.swift.codec.ThriftProtocolType.LIST;
import static com.facebook.swift.codec.ThriftProtocolType.MAP;
import static com.facebook.swift.codec.ThriftProtocolType.SET;
import static com.facebook.swift.codec.ThriftProtocolType.STRING;
import static com.facebook.swift.codec.ThriftProtocolType.STRUCT;
import static com.facebook.swift.codec.internal.compiler.byteCode.Access.*;
import static com.facebook.swift.codec.internal.compiler.byteCode.CaseStatement.caseStatement;
import static com.facebook.swift.codec.internal.compiler.byteCode.NamedParameterDefinition.arg;
import static com.facebook.swift.codec.internal.compiler.byteCode.ParameterizedType.type;

@NotThreadSafe
public class ThriftCodecByteCodeGenerator<T>
{
    private static final String PACKAGE = "$wift";

    private static final Map<ThriftProtocolType, Method> READ_METHODS;
    private static final Map<ThriftProtocolType, Method> WRITE_METHODS;

    private final ThriftCodecManager codecManager;
    private final ThriftStructMetadata<T> metadata;
    private final ParameterizedType structType;
    private final ParameterizedType codecType;

    private final ClassDefinition classDefinition;

    private final ConstructorParameters parameters = new ConstructorParameters();

    private final FieldDefinition typeField;
    private final Map<Short, FieldDefinition> codecFields;

    private final ThriftCodec<T> thriftCodec;

    @SuppressFBWarnings("DM_DEFAULT_ENCODING")
    public ThriftCodecByteCodeGenerator(
            ThriftCodecManager codecManager,
            ThriftStructMetadata<T> metadata,
            DynamicClassLoader classLoader,
            boolean debug
    )
    {
        this.codecManager = codecManager;
        this.metadata = metadata;

        structType = type(metadata.getStructClass());
        codecType = toCodecType(metadata);

        classDefinition = new ClassDefinition(
                a(PUBLIC, SUPER),
                codecType.getClassName(),
                type(StructThriftCodec.class, structType),
                type(ThriftCodec.class, structType)
        );

        // declare the class fields
        typeField = declareTypeField();
        codecFields = declareCodecFields();

        // declare methods
        defineConstructor();
        defineGetTypeMethod();
        defineReadMethod();
        defineWriteMethod();

        // add the non-generic bridge read and write methods
        defineReadBridgeMethod();
        defineWriteBridgeMethod();

        // generate the byte code
        ClassWriter cw = new ClassWriter(ClassWriter.COMPUTE_FRAMES);
        classDefinition.getClassNode().accept(cw);
        byte[] byteCode = cw.toByteArray();

        // Run the asm verifier only in debug mode (prints a ton of info)
        if (debug) {
            ClassReader reader = new ClassReader(byteCode);
            CheckClassAdapter.verify(reader, classLoader, true, new PrintWriter(System.out));
        }

        // load the class
        Class<?> codecClass = classLoader.defineClass(codecType.getClassName().replace('/', '.'), byteCode);
        try {
            Class<?>[] types = parameters.getTypes();
            Constructor<?> constructor = codecClass.getConstructor(types);
            thriftCodec = (ThriftCodec<T>) constructor.newInstance(parameters.getValues());
        }
        catch (Exception e) {
            throw new IllegalStateException("Generated class is invalid", e);
        }
    }

    public ThriftCodec<T> getThriftCodec()
    {
        return thriftCodec;
    }

    /**
     * Declares the private ThriftType field type.
     */
    private FieldDefinition declareTypeField()
    {
        FieldDefinition typeField = new FieldDefinition(a(PRIVATE, FINAL), "type", type(ThriftType.class));
        classDefinition.addField(typeField);

        // add constructor parameter to initialize this field
        parameters.add(typeField, ThriftType.struct(metadata));

        return typeField;
    }

    /**
     * Declares a field for each delegate codec
     *
     * @return a map from field id to the codec for the field
     */
    private Map<Short, FieldDefinition> declareCodecFields()
    {
        Map<Short, FieldDefinition> codecFields = new TreeMap<>();
        for (ThriftFieldMetadata fieldMetadata : metadata.getFields()) {
            if (needsCodec(fieldMetadata)) {

                ThriftCodec<?> codec = codecManager.getCodec(fieldMetadata.getType());
                String fieldName = fieldMetadata.getName() + "Codec";

                FieldDefinition codecField = new FieldDefinition(a(PRIVATE, FINAL), fieldName, type(codec.getClass()));
                classDefinition.addField(codecField);
                codecFields.put(fieldMetadata.getId(), codecField);

                parameters.add(codecField, codec);
            }
        }
        return codecFields;
    }

    /**
     * Defines the constructor with a parameter for the ThriftType and the delegate codecs.  The
     * constructor simply assigns these parameters to the class fields.
     */
    private void defineConstructor()
    {
        //
        // declare the constructor
        MethodDefinition constructor = new MethodDefinition(
                a(PUBLIC),
                "<init>",
                type(void.class),
                parameters.getParameters()
        );

        // invoke super (Object) constructor
        constructor.loadThis().invokeConstructor(type(StructThriftCodec.class));

        // this.foo = foo;
        for (FieldDefinition fieldDefinition : parameters.getFields()) {
            constructor.loadThis()
                    .loadVariable(fieldDefinition.getName())
                    .putField(codecType, fieldDefinition);
        }

        // return; (implicit)
        constructor.ret();

        classDefinition.addMethod(constructor);
    }

    /**
     * Defines the getType method which simply returns the value of the type field.
     */
    private void defineGetTypeMethod()
    {
        classDefinition.addMethod(
                new MethodDefinition(a(PUBLIC), "getType", type(ThriftType.class))
                        .loadThis()
                        .getField(codecType, typeField)
                        .retObject()
        );
    }

    /**
     * Defines the read method.
     */
    private void defineReadMethod()
    {
        MethodDefinition read = new MethodDefinition(
                a(PUBLIC),
                "read",
                structType,
                arg("protocol", TProtocol.class)
        ).addException(Exception.class);

        // TField currentField;
        read.addLocalVariable(type(TField.class), "currentField");

        // read all of the data in to local variables
        Map<Short, LocalVariableDefinition> structData = readFieldValues(read);

        // build the struct
        buildStruct(read, structData);
    }

    /**
     * Defines the code to read all of the data from the protocol into local variables.
     */
    private Map<Short, LocalVariableDefinition> readFieldValues(MethodDefinition read)
    {
        // declare and init local variables here
        Map<Short, LocalVariableDefinition> structData = new TreeMap<>();
        for (ThriftFieldMetadata field : metadata.getFields()) {
            LocalVariableDefinition variable = read.addInitializedLocalVariable(
                    toParameterizedType(field.getType()),
                    "f_" + field.getName()
            );
            structData.put(field.getId(), variable);
        }

        LocalVariableDefinition protocol = read.getLocalVariable("protocol");
        LocalVariableDefinition currentField = read.getLocalVariable("currentField");

        // protocol.readStructBegin()
        read.loadVariable(protocol).invokeVirtual(
                TProtocol.class,
                "readStructBegin",
                TStruct.class
        );
        read.pop();

        // begin while loop
        read.visitLabel("while-begin");
        // currentField = protocol.readFieldBegin()
        read.loadVariable(protocol).invokeVirtual(TProtocol.class, "readFieldBegin", TField.class);
        read.storeVariable(currentField);
        // if (currentField.type == TType.STOP) {
        //   break;
        // }
        read.loadVariable(currentField).getField(TField.class, "type", byte.class);
        read.loadConstant(TType.STOP);
        read.ifEqualGoto("while-end");

        // switch (currentField.id)
        read.loadVariable(currentField).getField(TField.class, "id", short.class);
        List<CaseStatement> cases = new ArrayList<>();
        for (ThriftFieldMetadata field : metadata.getFields()) {
            cases.add(caseStatement(field.getId(), field.getName() + "-field"));
        }
        read.switchStatement("default", cases);

        for (ThriftFieldMetadata field : metadata.getFields()) {
            // case field.id:
            read.visitLabel(field.getName() + "-field");

            // push ThriftTypeCodec for this field
            FieldDefinition codecField = codecFields.get(field.getId());
            if (codecField != null) {
                read.loadThis().getField(codecType, codecField);
            }

            // push protocol (don't need a codec for simple types)
            read.loadVariable(protocol);

            // read value
            Method readMethod = READ_METHODS.get(field.getType().getProtocolType());
            if (readMethod == null) {
                throw new IllegalArgumentException("Unsupported field type " + field.getType().getProtocolType());
            }
            read.invokeVirtual(readMethod);

            // todo this cast should be based on readMethod return type and fieldType (or coercion type)
            // add cast if necessary
            if (needsCastAfterRead(field, readMethod)) {
                read.checkCast(toParameterizedType(field.getType()));
            }

            // coerce the type
            if (field.getCoercion() != null) {
                read.invokeStatic(field.getCoercion().getFromThrift());
            }

            // store protocol value
            read.storeVariable(structData.get(field.getId()));

            // go back to top of loop
            read.gotoLabel("while-begin");
        }

        // default case
        read.visitLabel("default");
        read.loadVariable(protocol);
        read.loadVariable(currentField).getField(TField.class, "type", byte.class);
        read.invokeStatic(TProtocolUtil.class, "skip", void.class, TProtocol.class, byte.class);
        read.gotoLabel("while-begin");

        // end of while loop
        read.visitLabel("while-end");

        read.loadVariable(protocol).invokeVirtual(TProtocol.class, "readStructEnd", void.class);
        return structData;
    }

    /**
     * Defines the code to build the struct instance using the data in the local variables.
     */
    private void buildStruct(MethodDefinition read, Map<Short, LocalVariableDefinition> structData)
    {

        // construct the instance and store it in the instance local variable
        LocalVariableDefinition instance = constructInstance(read, structData);

        // inject fields
        injectFields(read, instance, structData);

        // inject methods
        injectMethods(read, instance, structData);

        // invoke factory method if present
        invokeFactoryMethod(read, structData, instance);

        // push instance on stack, and return it
        read.loadVariable(instance)
                .retObject();

        classDefinition.addMethod(read);
    }

    /**
     * Defines the code to construct the struct (or builder) instance and stores it in a local
     * variable.
     */
    private LocalVariableDefinition constructInstance(MethodDefinition read, Map<Short, LocalVariableDefinition> structData)
    {
        LocalVariableDefinition instance = read.addLocalVariable(structType, "instance");

        // create the new instance (or builder)
        if (metadata.getBuilderClass() == null) {
            read.newObject(structType).dup();
        }
        else {
            read.newObject(metadata.getBuilderClass()).dup();
        }

        // invoke constructor
        ThriftConstructorInjection constructor = metadata.getConstructor();
        // push parameters on stack
        for (ThriftParameterInjection parameter : constructor.getParameters()) {
            read.loadVariable(structData.get(parameter.getId()));
        }
        // invoke constructor
        read.invokeConstructor(constructor.getConstructor())
                .storeVariable(instance);
        return instance;
    }

    /**
     * Defines the code to inject data into the struct public fields.
     */
    private void injectFields(MethodDefinition read, LocalVariableDefinition instance, Map<Short, LocalVariableDefinition> structData)
    {
        for (ThriftFieldMetadata field : metadata.getFields()) {
            for (ThriftInjection injection : field.getInjections()) {
                if (injection instanceof ThriftFieldInjection) {

                    ThriftFieldInjection fieldInjection = (ThriftFieldInjection) injection;

                    // if field is an Object && field != null
                    if (!isProtocolTypeJavaPrimitive(field)) {
                        read.loadVariable(structData.get(field.getId()))
                                .ifNullGoto("field_is_null_" + field.getName());
                    }

                    // write value
                    read.loadVariable(instance)
                            .loadVariable(structData.get(field.getId()))
                            .putField(fieldInjection.getField());

                    // else do nothing
                    if (!isProtocolTypeJavaPrimitive(field)) {
                        read.visitLabel("field_is_null_" + field.getName());
                    }
                }
            }
        }
    }

    /**
     * Defines the code to inject data into the struct methods.
     */
    private void injectMethods(MethodDefinition read, LocalVariableDefinition instance, Map<Short, LocalVariableDefinition> structData)
    {
        for (ThriftMethodInjection methodInjection : metadata.getMethodInjections()) {
            // if any parameter is non-null, invoke the method
            for (ThriftParameterInjection parameter : methodInjection.getParameters()) {
                if (!isParameterTypeJavaPrimitive(parameter)) {
                    read.loadVariable(structData.get(parameter.getId()));
                    read.ifNotNullGoto("invoke_" + methodInjection.getMethod().toGenericString());
                }
                else {
                    read.gotoLabel("invoke_" + methodInjection.getMethod().toGenericString());
                }
            }
            read.gotoLabel("skip_invoke_" + methodInjection.getMethod().toGenericString());

            // invoke the method
            read.visitLabel("invoke_" + methodInjection.getMethod().toGenericString());
            read.loadVariable(instance);

            // push parameters on stack
            for (ThriftParameterInjection parameter : methodInjection.getParameters()) {
                read.loadVariable(structData.get(parameter.getId()));
            }

            // invoke the method
            read.invokeVirtual(methodInjection.getMethod());

            // if method has a return, we need to pop it off the stack
            if (methodInjection.getMethod().getReturnType() != void.class) {
                read.pop();
            }

            // skip invocation
            read.visitLabel("skip_invoke_" + methodInjection.getMethod().toGenericString());
        }
    }

    /**
     * Defines the code that calls the builder factory method.
     */
    private void invokeFactoryMethod(MethodDefinition read, Map<Short, LocalVariableDefinition> structData, LocalVariableDefinition instance)
    {
        ThriftMethodInjection builderMethod = metadata.getBuilderMethod();
        if (builderMethod != null) {
            read.loadVariable(instance);

            // push parameters on stack
            for (ThriftParameterInjection parameter : builderMethod.getParameters()) {
                read.loadVariable(structData.get(parameter.getId()));
            }

            // invoke the method
            read.invokeVirtual(builderMethod.getMethod())
                    .storeVariable(instance);
        }
    }

    /**
     * Define the write method.
     */
    private void defineWriteMethod()
    {
        MethodDefinition write = new MethodDefinition(
                a(PUBLIC),
                "write",
                null,
                arg("struct", structType),
                arg("protocol", TProtocol.class)
        );
        classDefinition.addMethod(write);

        // TProtocolReader writer = new TProtocolWriter(protocol);
        write.addLocalVariable(type(TProtocolWriter.class), "writer");
        write.newObject(TProtocolWriter.class);
        write.dup();
        write.loadVariable("protocol");
        write.invokeConstructor(type(TProtocolWriter.class), type(TProtocol.class));
        write.storeVariable("writer");

        LocalVariableDefinition protocol = write.getLocalVariable("writer");

        // protocol.writeStructBegin("bonk");
        write.loadVariable(protocol)
                .loadConstant(metadata.getStructName())
                .invokeVirtual(TProtocolWriter.class, "writeStructBegin", void.class, String.class);

        // write fields
        for (ThriftFieldMetadata field : metadata.getFields()) {
            // push protocol
            write.loadVariable(protocol);

            // push (String) field.name
            write.loadConstant(field.getName());

            // push (short) field.id
            write.loadConstant(field.getId());

            // push ThriftTypeCodec for this field
            FieldDefinition codecField = codecFields.get(field.getId());
            if (codecField != null) {
                write.loadThis().getField(codecType, codecField);
            }

            // push field value
            loadFieldValue(write, field);

            // if field value is null, don't coerce or write the field
            if (!isFieldTypeJavaPrimitive(field)) {
                // ifNullGoto consumes the top of the stack, so we need to duplicate the value
                write.dup();
                write.ifNullGoto("field_is_null_" + field.getName());
            }

            // coerce value
            if (field.getCoercion() != null) {
                write.invokeStatic(field.getCoercion().getToThrift());

                // if coerced value is null, don't write the field
                if (!isProtocolTypeJavaPrimitive(field)) {
                    write.dup();
                    write.ifNullGoto("field_is_null_" + field.getName());
                }
            }

            // write value
            Method writeMethod = WRITE_METHODS.get(field.getType().getProtocolType());
            if (writeMethod == null) {
                throw new IllegalArgumentException(
                        "Unsupported field type " + field.getType().getProtocolType()
                );
            }
            write.invokeVirtual(writeMethod);

            //
            // If not written because of a null, clean-up the stack
            if (!isProtocolTypeJavaPrimitive(field) || !isFieldTypeJavaPrimitive(field)) {

                // value was written so skip cleanup
                write.gotoLabel("field_end_" + field.getName());

                // cleanup stack for null field value
                write.visitLabel("field_is_null_" + field.getName());
                // pop value
                write.pop();
                // pop codec
                if (codecField != null) {
                    write.pop();
                }
                // pop id
                write.pop();
                // pop name
                write.pop();
                // pop protocol
                write.pop();

                write.visitLabel("field_end_" + field.getName());
            }
        }

        write.loadVariable(protocol)
                .invokeVirtual(TProtocolWriter.class, "writeStructEnd", void.class);

        write.ret();
    }

    private void loadFieldValue(MethodDefinition write, ThriftFieldMetadata field)
    {
        write.loadVariable("struct");
        ThriftExtraction extraction = field.getExtraction();
        if (extraction instanceof ThriftFieldExtractor) {
            ThriftFieldExtractor fieldExtractor = (ThriftFieldExtractor) extraction;
            write.getField(fieldExtractor.getField());
        }
        else if (extraction instanceof ThriftMethodExtractor) {
            ThriftMethodExtractor methodExtractor = (ThriftMethodExtractor) extraction;
            write.invokeVirtual(methodExtractor.getMethod());
        }
    }

    /**
     * Defines the generics bridge method with untyped args to the type specific read method.
     */
    private void defineReadBridgeMethod()
    {
        classDefinition.addMethod(
                new MethodDefinition(a(PUBLIC, BRIDGE, SYNTHETIC), "read", type(Object.class), arg("protocol", TProtocol.class))
                        .addException(Exception.class)
                        .loadThis()
                        .loadVariable("protocol")
                        .invokeVirtual(codecType, "read", structType, type(TProtocol.class))
                        .retObject()
        );
    }

    /**
     * Defines the generics bridge method with untyped args to the type specific write method.
     */
    private void defineWriteBridgeMethod()
    {
        classDefinition.addMethod(
                new MethodDefinition(a(PUBLIC, BRIDGE, SYNTHETIC), "write", null, arg("struct", Object.class), arg("protocol", TProtocol.class))
                        .addException(Exception.class)
                        .loadThis()
                        .loadVariable("struct", structType)
                        .loadVariable("protocol")
                        .invokeVirtual(
                                codecType,
                                "write",
                                type(void.class),
                                structType,
                                type(TProtocol.class)
                        )
                        .ret()
        );
    }

    private boolean isParameterTypeJavaPrimitive(ThriftParameterInjection parameter)
    {
        return isJavaPrimitive(TypeToken.of(parameter.getJavaType()));
    }

    private boolean isFieldTypeJavaPrimitive(ThriftFieldMetadata field)
    {
        return isJavaPrimitive(TypeToken.of(field.getType().getJavaType()));
    }

    private boolean isProtocolTypeJavaPrimitive(ThriftFieldMetadata field)
    {
        if (field.getType().isCoerced()) {
            return isJavaPrimitive(TypeToken.of(field.getType().getUncoercedType().getJavaType()));
        }
        else {
            return isJavaPrimitive(TypeToken.of(field.getType().getJavaType()));
        }
    }

    private boolean isJavaPrimitive(TypeToken<?> typeToken)
    {
        return typeToken
                .getRawType()
                .isPrimitive();
    }

    private static boolean needsCastAfterRead(ThriftFieldMetadata field, Method readMethod)
    {
        Class<?> methodReturn = readMethod.getReturnType();
        Class<?> fieldType;
        if (field.getCoercion() != null) {
            fieldType = field.getCoercion().getFromThrift().getParameterTypes()[0];
        }
        else {
            fieldType = TypeToken.of(field.getType().getJavaType()).getRawType();
        }
        boolean needsCast = !fieldType.isAssignableFrom(methodReturn);
        return needsCast;
    }

    private boolean needsCodec(ThriftFieldMetadata fieldMetadata)
    {
        ThriftProtocolType protocolType = fieldMetadata.getType().getProtocolType();
        return protocolType == ENUM ||
                protocolType == STRUCT ||
                protocolType == SET ||
                protocolType == LIST ||
                protocolType == MAP;
    }

    private ParameterizedType toCodecType(ThriftStructMetadata<?> metadata)
    {
        return type(PACKAGE + "/" + type(metadata.getStructClass()).getClassName() + "Codec");
    }

    private static class ConstructorParameters
    {
        private final List<FieldDefinition> fields = new ArrayList<>();
        private final List<Object> values = new ArrayList<>();

        private void add(FieldDefinition field, Object value)
        {
            fields.add(field);
            values.add(value);
        }

        public List<FieldDefinition> getFields()
        {
            return fields;
        }

        public Object[] getValues()
        {
            return values.toArray(new Object[values.size()]);
        }

        public List<NamedParameterDefinition> getParameters()
        {
            return Lists.transform(fields, new Function<FieldDefinition, NamedParameterDefinition>()
            {
                public NamedParameterDefinition apply(FieldDefinition field)
                {
                    return arg(field.getName(), field.getType());
                }
            });
        }

        public Class<?>[] getTypes()
        {
            List<Class<?>> types = Lists.transform(values, new Function<Object, Class<?>>()
            {
                public Class<?> apply(Object value)
                {
                    return value.getClass();
                }
            });

            return types.toArray(new Class<?>[types.size()]);
        }
    }

    public static ParameterizedType toParameterizedType(ThriftType type)
    {
        switch (type.getProtocolType()) {
            case BOOL:
            case BYTE:
            case DOUBLE:
            case I16:
            case I32:
            case I64:
            case STRING:
            case STRUCT:
            case ENUM:
                return type((Class<?>) type.getJavaType());
            case MAP:
                return type(Map.class, toParameterizedType(type.getKeyType()), toParameterizedType(type.getValueType()));
            case SET:
                return type(Set.class, toParameterizedType(type.getValueType()));
            case LIST:
                return type(List.class, toParameterizedType(type.getValueType()));
            default:
                throw new IllegalArgumentException("Unsupported thrift field type " + type);
        }
    }

    static {
        ImmutableMap.Builder<ThriftProtocolType, Method> writeBuilder = ImmutableMap.builder();
        ImmutableMap.Builder<ThriftProtocolType, Method> readBuilder = ImmutableMap.builder();

        try {
            writeBuilder.put(BOOL, TProtocolWriter.class.getMethod("writeBoolField", String.class, short.class, boolean.class));
            writeBuilder.put(BYTE, TProtocolWriter.class.getMethod("writeByteField", String.class, short.class, byte.class));
            writeBuilder.put(DOUBLE, TProtocolWriter.class.getMethod("writeDoubleField", String.class, short.class, double.class));
            writeBuilder.put(I16, TProtocolWriter.class.getMethod("writeI16Field", String.class, short.class, short.class));
            writeBuilder.put(I32, TProtocolWriter.class.getMethod("writeI32Field", String.class, short.class, int.class));
            writeBuilder.put(I64, TProtocolWriter.class.getMethod("writeI64Field", String.class, short.class, long.class));
            writeBuilder.put(STRING, TProtocolWriter.class.getMethod("writeBinaryField", String.class, short.class, ByteBuffer.class));
            writeBuilder.put(STRUCT, TProtocolWriter.class.getMethod("writeStructField", String.class, short.class, ThriftCodec.class, Object.class));
            writeBuilder.put(MAP, TProtocolWriter.class.getMethod("writeMapField", String.class, short.class, ThriftCodec.class, Map.class));
            writeBuilder.put(SET, TProtocolWriter.class.getMethod("writeSetField", String.class, short.class, ThriftCodec.class, Set.class));
            writeBuilder.put(LIST, TProtocolWriter.class.getMethod("writeListField", String.class, short.class, ThriftCodec.class, List.class));
            writeBuilder.put(ENUM, TProtocolWriter.class.getMethod("writeEnumField", String.class, short.class, ThriftCodec.class, Enum.class));

            readBuilder.put(BOOL, TProtocol.class.getMethod("readBool"));
            readBuilder.put(BYTE, TProtocol.class.getMethod("readByte"));
            readBuilder.put(DOUBLE, TProtocol.class.getMethod("readDouble"));
            readBuilder.put(I16, TProtocol.class.getMethod("readI16"));
            readBuilder.put(I32, TProtocol.class.getMethod("readI32"));
            readBuilder.put(I64, TProtocol.class.getMethod("readI64"));
            readBuilder.put(STRING, TProtocol.class.getMethod("readBinary"));
            readBuilder.put(STRUCT, StructThriftCodec.class.getMethod("read", TProtocol.class));
            readBuilder.put(MAP, MapThriftCodec.class.getMethod("read", TProtocol.class));
            readBuilder.put(SET, SetThriftCodec.class.getMethod("read", TProtocol.class));
            readBuilder.put(LIST, ListThriftCodec.class.getMethod("read", TProtocol.class));
            readBuilder.put(ENUM, EnumThriftCodec.class.getMethod("read", TProtocol.class));
        }
        catch (NoSuchMethodException e) {
            throw Throwables.propagate(e);
        }
        WRITE_METHODS = writeBuilder.build();
        READ_METHODS = readBuilder.build();
    }
}
