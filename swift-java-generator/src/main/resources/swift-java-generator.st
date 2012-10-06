prologue(package) ::= <<
package <package>;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;

import com.facebook.swift.codec.ThriftEnumValue;
import com.facebook.swift.codec.ThriftField;
import com.facebook.swift.codec.ThriftStruct;
import com.facebook.swift.service.ThriftException;
import com.facebook.swift.service.ThriftMethod;
import com.facebook.swift.service.ThriftService;

import org.apache.thrift.TException;
>>

thriftservice(package, service) ::= <<
<prologue(package)>

@ThriftService(value = "<service.name>")
public interface <service.name> {
    <service.methods:thriftmethod(); separator="\n\n">
}<\n>
>>

thriftstructdefinition(package, struct) ::= <<
<prologue(package)>

@ThriftStruct
public class <struct.name> {
    <thriftstructfieldlist(struct.fields)>
}<\n>
>>

thriftexceptiondefinition(package, exception) ::= <<
<prologue(package)>

@ThriftStruct
public class <exception.name> extends Exception {
    <thriftstructfieldlist(exception.fields)>
}<\n>
>>

thriftenumeration(package, enum) ::= <<
<prologue(package)>

public enum <enum.name> {
    <thriftenumerationvaluelist(enum.fields)>;

    private final int value;

    private <enum.name>(int value) {
        this.value = value;
    }

    @ThriftEnumValue
    public long getValue() {
        return value;
    }
}<\n>
>>

thriftmethod(method) ::= <%
	@ThriftMethod(value = "<method.name>", oneway = <method.oneway>, exception = {<thriftexceptionlist(method.throwsFields)>})
	<\n>
	public void <method.name>(<thriftmethodarglist(method.arguments)>)
	<javaexceptionspecification(method)>
	;
%>

javaexceptionspecification(method) ::= <%
	<if(method.throwsTException)>
		<\ >throws TException
		<if (method.throwsFields)>
			, <method.throwsFields:{throwsField | <throwsField.type>}; separator=", ">
		<endif>
	<elseif(method.throwsFields)>
		<\ >throws <method.throwsFields:{throwsField | <throwsField.type>}; separator=", ">
	<endif>
%>

thriftexceptionlist(exceptions) ::= <%
    <exceptions:thriftexception(); separator=", ">
%>

thriftexception(exception) ::= <%
    @ThriftException(type = <exception.type>.class, id = <exception.identifier>)
%>

thriftmethodarglist(args) ::= <%
    <if (args)><\n><args:{arg | <thriftmethodarg(arg, i)>}; separator=",\n"><\n><endif>
%>

thriftmethodarg(arg, i) ::= <<
	@ThriftField(value = <i>) <arg.type> <arg.name>
>>

thriftenumerationvaluelist(fields) ::= "<fields:thriftenumerationvalue(); separator=\",\n\">"

thriftenumerationvalue(field) ::= <%
	<field.name><if(field.value)>(<field.value>)<endif>
%>

thriftstructfieldlist(fields) ::= <%
	<fields:thriftstructfield(); separator="\n">
%>

thriftstructfield(field) ::= <%
	@ThriftField(value = <field.identifier>, required = <field.required>) public <field.type> <field.name>;
%>
