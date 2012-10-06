package com.facebook.swift.swiftify;

import com.facebook.swift.parser.model.Const;
import com.facebook.swift.parser.model.Definition;
import com.facebook.swift.parser.model.Document;
import com.facebook.swift.parser.model.Header;
import com.facebook.swift.parser.model.IntegerEnum;
import com.facebook.swift.parser.model.Namespace;
import com.facebook.swift.parser.model.Service;
import com.facebook.swift.parser.model.Struct;
import com.facebook.swift.parser.model.ThriftException;
import com.facebook.swift.parser.model.ThriftField;
import com.facebook.swift.parser.model.ThriftMethod;
import com.facebook.swift.parser.model.ThriftType;
import com.facebook.swift.parser.model.Typedef;
import com.facebook.swift.swiftify.util.ThriftMethodAdaptor;
import com.facebook.swift.swiftify.util.RequiredAttributeRenderer;
import com.facebook.swift.swiftify.util.ThriftTypeAttributeRenderer;
import com.google.common.base.Charsets;
import com.google.common.io.Resources;
import com.google.inject.Inject;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.STGroupString;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class SwiftClassGenerator extends AbstractIdlNodeVisitor
{
    protected static final Logger logger = LoggerFactory.getLogger(SwiftClassGenerator.class);

    private final STGroupString templateGroup;
    private String packageToGenerate;
    private String generatedSourceRoot;

    private final Map<String, ThriftType> typedefMap = new HashMap<>();
    @Inject
    public SwiftClassGenerator(String generatedSourceRoot, boolean declareThriftMethodThrowsThriftException)
            throws IOException
    {
        String resourceString = Resources.toString(Resources.getResource("swift-java-generator.st"), Charsets.UTF_8);
        templateGroup = new STGroupString(resourceString);
        templateGroup.registerRenderer(ThriftType.class, new ThriftTypeAttributeRenderer(typedefMap));
        templateGroup.registerRenderer(ThriftField.Required.class, new RequiredAttributeRenderer());
        templateGroup.registerModelAdaptor(ThriftMethod.class, new ThriftMethodAdaptor(declareThriftMethodThrowsThriftException));

        this.generatedSourceRoot = generatedSourceRoot;
    }

    protected void generateFromDocument(Document node)
            throws IOException
    {
        parseHeader(node.getHeader());

        for (Definition definition : node.getDefinitions()) {
            generate(definition);
        }
    }

    @Override
    protected void parseHeader(Header node)
            throws IOException
    {
        Namespace javaNamespace = null;

        for (Namespace namespace : node.getNamespaces()) {
            if (namespace.getType().compareTo("java") == 0) {
                if (javaNamespace != null) {
                    throw new IllegalStateException("Multiple java namespaces specified");
                }
                javaNamespace = namespace;
            }
        }

        if (javaNamespace == null) {
            packageToGenerate = "";
        } else {
            packageToGenerate = javaNamespace.getValue();
        }
    }

    protected void generate(Definition definition)
            throws IOException
    {
        if (definition instanceof Service) {
            generateService((Service)definition);
        } else if (definition instanceof Struct) {
            generateStruct((Struct)definition);
        } else if (definition instanceof ThriftException) {
            generateThriftException((ThriftException)definition);
        } else if (definition instanceof IntegerEnum) {
            generateIntegerEnum((IntegerEnum)definition);
        } else if (definition instanceof Typedef) {
            generateTypedef((Typedef)definition);
        } else if (definition instanceof Const) {
            generateConst((Const)definition);
        } else {
            throw new IllegalStateException("Don't know how to generate code for " +
                                            definition.getClass().getSimpleName());
        }
    }

    @Override
    protected void generateService(Service node)
            throws IOException
    {
        try (FileWriter fileWriter = createSourceFile(node.getName())) {
            ST st = templateGroup.getInstanceOf("thriftservice");
            st.add("package", packageToGenerate);
            st.add("service", node);
            fileWriter.write(st.render());
        }
    }

    @Override
    protected void generateStruct(Struct node)
            throws IOException
    {
        try (FileWriter fileWriter = createSourceFile(node.getName())) {
            ST st = templateGroup.getInstanceOf("thriftstructdefinition");
            st.add("package", packageToGenerate);
            st.add("struct", node);
            fileWriter.write(st.render());
        }
    }

    @Override
    protected void generateThriftException(ThriftException node)
            throws IOException
    {
        try (FileWriter fileWriter = createSourceFile(node.getName())) {
            ST st = templateGroup.getInstanceOf("thriftexceptiondefinition");
            st.add("package", packageToGenerate);
            st.add("exception", node);
            fileWriter.write(st.render());
        }
    }

    @Override
    protected void generateIntegerEnum(IntegerEnum node)
            throws IOException
    {
        try (FileWriter fileWriter = createSourceFile(node.getName())) {
            ST st = templateGroup.getInstanceOf("thriftenumeration");
            st.add("package", packageToGenerate);
            st.add("enum", node);
            fileWriter.write(st.render());
        }
    }

    @Override
    protected void generateTypedef(Typedef node)
    {
        // Typedefs won't actually generate new code, they are just added to a map so they
        // can be looked up later to get real types.
        typedefMap.put(node.getName(), node.getType());
    }

    @Override
    protected void generateConst(Const node)
    {
        // TODO(andrewcox): This gets called, but I'm ignoring it for now. The right thing to do
        // would probably be to build these up in a list, and then dump all the definitions to
        // a Constants.java file at the end.

        logger.warn("Constants are not supported, skipping generation of " +
                    node.getName() + " in package " + packageToGenerate);
    }

    private FileWriter createSourceFile(String filename)
            throws IOException
    {
        File sourceFilePath = new File(generatedSourceRoot,
                                       convertNamespaceToRelativePath(packageToGenerate));
        if (!sourceFilePath.exists() && !sourceFilePath.mkdirs()) {
            throw new IllegalArgumentException("Could not create directory " + sourceFilePath);
        }
        File sourceFile = new File(sourceFilePath, filename + ".java");
        return new FileWriter(sourceFile);
    }

    private String convertNamespaceToRelativePath(String packageToGenerate)
    {
        return StringUtils.join(StringUtils.split(packageToGenerate, "."), "/");
    }
}
