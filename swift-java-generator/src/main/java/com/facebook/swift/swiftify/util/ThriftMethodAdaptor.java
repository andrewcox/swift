package com.facebook.swift.swiftify.util;

import com.facebook.swift.parser.model.ThriftMethod;
import com.google.inject.Inject;
import org.stringtemplate.v4.Interpreter;
import org.stringtemplate.v4.ModelAdaptor;
import org.stringtemplate.v4.ST;
import org.stringtemplate.v4.misc.STNoSuchPropertyException;

public class ThriftMethodAdaptor implements ModelAdaptor
{
    private final boolean declareThriftMethodThrowsThriftException;

    @Inject
    public ThriftMethodAdaptor(boolean declareThriftMethodThrowsThriftException)
    {
        this.declareThriftMethodThrowsThriftException = declareThriftMethodThrowsThriftException;
    }

    @Override
    public Object getProperty(Interpreter interp, ST self, Object o, Object property, String propertyName)
            throws STNoSuchPropertyException
    {
        ThriftMethod m = (ThriftMethod) o;

        if (propertyName.equals("name")) {
            return m.getName();
        } else if (propertyName.equals("oneway")) {
            return m.isOneway();
        } else if (propertyName.equals("throwsFields")) {
            return m.getThrowsFields();
        } else if (propertyName.equals("arguments")) {
            return m.getArguments().size() == 0 ? null : m.getArguments();
        } else if (propertyName.equals("throwsTException")) {
            return declareThriftMethodThrowsThriftException;
        }

        return null;
    }
}
