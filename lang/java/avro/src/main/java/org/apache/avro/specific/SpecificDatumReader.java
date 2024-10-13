/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.avro.specific;

import org.apache.avro.Conversion;
import org.apache.avro.Schema;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.io.ResolvingDecoder;
import org.apache.avro.util.ClassUtils;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * {@link org.apache.avro.io.DatumReader DatumReader} for generated Java
 * classes.
 */
public class SpecificDatumReader<T> extends GenericDatumReader<T> {

  public static final String[] DENY_LIST;

  static {
    DENY_LIST = new String[] { "javax.script.ScriptEngineManager", "URLClassLoader", "bsh.XThis", "bsh.Interpreter",
        "com.mchange.v2.c3p0.PoolBackedDataSource", "com.mchange.v2.c3p0.impl.PoolBackedDataSourceBase",
        "clojure.lang.PersistentArrayMap", "clojure.inspector.proxy$javax.swing.table.AbstractTableModel$ff19274a",
        "org.apache.commons.beanutils.BeanComparator", "org.apache.commons.collections.Transformer",
        "org.apache.commons.collections.functors.ChainedTransformer",
        "org.apache.commons.collections.functors.ConstantTransformer",
        "org.apache.commons.collections.functors.InstantiateTransformer", "org.apache.commons.collections.map.LazyMap",
        "org.apache.commons.collections.functors.InvokerTransformer",
        "org.apache.commons.collections.keyvalue.TiedMapEntry",
        "org.apache.commons.collections4.comparators.TransformingComparator",
        "org.apache.commons.collections4.functors.InvokerTransformer",
        "org.apache.commons.collections4.functors.ChainedTransformer",
        "org.apache.commons.collections4.functors.ConstantTransformer",
        "org.apache.commons.collections4.functors.InstantiateTransformer",
        "org.apache.commons.fileupload.disk.DiskFileItem", "org.apache.commons.io.output.DeferredFileOutputStream",
        "org.apache.commons.io.output.ThresholdingOutputStream", "org.apache.wicket.util.upload.DiskFileItem",
        "org.apache.wicket.util.io.DeferredFileOutputStream", "org.apache.wicket.util.io.ThresholdingOutputStream",
        "org.codehaus.groovy.runtime.ConvertedClosure", "org.codehaus.groovy.runtime.MethodClosure",
        "org.hibernate.engine.spi.TypedValue", "org.hibernate.tuple.component.AbstractComponentTuplizer",
        "org.hibernate.tuple.component.PojoComponentTuplizer", "org.hibernate.type.AbstractType",
        "org.hibernate.type.ComponentType", "org.hibernate.type.Type", "org.hibernate.EntityMode",
        "com.sun.rowset.JdbcRowSetImpl", "org.jboss.interceptor.builder.InterceptionModelBuilder",
        "org.jboss.interceptor.builder.MethodReference", "org.jboss.interceptor.proxy.DefaultInvocationContextFactory",
        "org.jboss.interceptor.proxy.InterceptorMethodHandler",
        "org.jboss.interceptor.reader.ClassMetadataInterceptorReference",
        "org.jboss.interceptor.reader.DefaultMethodMetadata", "org.jboss.interceptor.reader.ReflectiveClassMetadata",
        "org.jboss.interceptor.reader.SimpleInterceptorMetadata",
        "org.jboss.interceptor.spi.instance.InterceptorInstantiator",
        "org.jboss.interceptor.spi.metadata.InterceptorReference", "org.jboss.interceptor.spi.metadata.MethodMetadata",
        "org.jboss.interceptor.spi.model.InterceptionType", "org.jboss.interceptor.spi.model.InterceptionModel",
        "sun.rmi.server.UnicastRef", "sun.rmi.transport.LiveRef", "sun.rmi.transport.tcp.TCPEndpoint",
        "java.rmi.server.RemoteObject", "java.rmi.server.RemoteRef", "java.rmi.server.UnicastRemoteObject",
        "sun.rmi.server.ActivationGroupImpl", "sun.rmi.server.UnicastServerRef",
        "org.springframework.aop.framework.AdvisedSupport", "net.sf.json.JSONObject",
        "org.jboss.weld.interceptor.builder.InterceptionModelBuilder",
        "org.jboss.weld.interceptor.builder.MethodReference",
        "org.jboss.weld.interceptor.proxy.DefaultInvocationContextFactory",
        "org.jboss.weld.interceptor.proxy.InterceptorMethodHandler",
        "org.jboss.weld.interceptor.reader.ClassMetadataInterceptorReference",
        "org.jboss.weld.interceptor.reader.DefaultMethodMetadata",
        "org.jboss.weld.interceptor.reader.ReflectiveClassMetadata",
        "org.jboss.weld.interceptor.reader.SimpleInterceptorMetadata",
        "org.jboss.weld.interceptor.spi.instance.InterceptorInstantiator",
        "org.jboss.weld.interceptor.spi.metadata.InterceptorReference",
        "org.jboss.weld.interceptor.spi.metadata.MethodMetadata",
        "org.jboss.weld.interceptor.spi.model.InterceptionModel",
        "org.jboss.weld.interceptor.spi.model.InterceptionType", "org.python.core.PyObject",
        "org.python.core.PyBytecode", "org.python.core.PyFunction", "org.mozilla.javascript",
        "org.apache.myfaces.context.servlet.FacesContextImpl",
        "org.apache.myfaces.context.servlet.FacesContextImplBase", "org.apache.myfaces.el.CompositeELResolver",
        "org.apache.myfaces.el.unified.FacesELContext",
        "org.apache.myfaces.view.facelets.el.ValueExpressionMethodExpression",
        "com.sun.syndication.feed.impl.ObjectBean", "org.springframework.beans.factory.ObjectFactory",
        "org.springframework.aop.framework.AdvisedSupport", "org.springframework.aop.target.SingletonTargetSource",
        "com.vaadin.data.util.NestedMethodProperty", "com.vaadin.data.util.PropertysetItem",
        "org.springframework.beans.factory.config.PropertyPathFactoryBean",
        "org.springframework.aop.support.DefaultBeanFactoryPointcutAdvisor",
        "javax.management.BadAttributeValueExpException", "org.apache.commons.configuration.ConfigurationMap",
        "com.mchange.v2.c3p0.WrapperConnectionPoolDataSource", "com.mchange.v2.c3p0.JndiRefForwardingDataSource",
        "com.sun.rowset.JdbcRowSetImpl", "org.eclipse.jetty.plus.jndi.Resource" };
  }

  private final List<String> untrustedPackages = new ArrayList<>();

  public SpecificDatumReader() {
    this(null, null, SpecificData.get());
  }

  /** Construct for reading instances of a class. */
  public SpecificDatumReader(Class<T> c) {
    this(SpecificData.getForClass(c));
    setSchema(getSpecificData().getSchema(c));
  }

  /** Construct where the writer's and reader's schemas are the same. */
  public SpecificDatumReader(Schema schema) {
    this(schema, schema, SpecificData.getForSchema(schema));
  }

  /** Construct given writer's and reader's schema. */
  public SpecificDatumReader(Schema writer, Schema reader) {
    this(writer, reader, SpecificData.getForSchema(reader));
  }

  /**
   * Construct given writer's schema, reader's schema, and a {@link SpecificData}.
   */
  public SpecificDatumReader(Schema writer, Schema reader, SpecificData data) {
    super(writer, reader, data);
    untrustedPackages.addAll(Arrays.asList(DENY_LIST));
  }

  /** Construct given a {@link SpecificData}. */
  public SpecificDatumReader(SpecificData data) {
    super(data);
  }

  /** Return the contained {@link SpecificData}. */
  public SpecificData getSpecificData() {
    return (SpecificData) getData();
  }

  @Override
  public void setSchema(Schema actual) {
    // if expected is unset and actual is a specific record,
    // then default expected to schema of currently loaded class
    if (getExpected() == null && actual != null && actual.getType() == Schema.Type.RECORD) {
      SpecificData data = getSpecificData();
      Class c = data.getClass(actual);
      if (c != null && SpecificRecord.class.isAssignableFrom(c))
        setExpected(data.getSchema(c));
    }
    super.setSchema(actual);
  }

  @Override
  protected Class findStringClass(Schema schema) {
    Class stringClass = null;
    switch (schema.getType()) {
    case STRING:
      stringClass = getPropAsClass(schema, SpecificData.CLASS_PROP);
      break;
    case MAP:
      stringClass = getPropAsClass(schema, SpecificData.KEY_CLASS_PROP);
      break;
    }
    if (stringClass != null)
      return stringClass;
    return super.findStringClass(schema);
  }

  private Class getPropAsClass(Schema schema, String prop) {
    String name = schema.getProp(prop);
    if (name == null)
      return null;
    try {
      Class clazz = ClassUtils.forName(getData().getClassLoader(), name);
      checkSecurity(clazz);
      return clazz;
    } catch (ClassNotFoundException e) {
      throw new AvroRuntimeException(e);
    }
  }

  private void checkSecurity(Class clazz) throws ClassNotFoundException {
    if (clazz.isPrimitive()) {
      return;
    }

    Package thePackage = clazz.getPackage();
    if (thePackage != null) {
      for (String untrustedPackage : getUntrustedPackages()) {
        if (thePackage.getName().equals(untrustedPackage) || thePackage.getName().startsWith(untrustedPackage + ".")) {
          throw new SecurityException(
              "Forbidden " + clazz + "! This class is not trusted to be included in Avro schema using java-class.");
        }
      }
    }
  }

  public final List<String> getUntrustedPackages() {
    return untrustedPackages;
  }

  @Override
  protected Object readRecord(Object old, Schema expected, ResolvingDecoder in) throws IOException {
    SpecificData data = getSpecificData();
    if (data.useCustomCoders()) {
      old = data.newRecord(old, expected);
      if (old instanceof SpecificRecordBase) {
        SpecificRecordBase d = (SpecificRecordBase) old;
        if (d.hasCustomCoders()) {
          d.customDecode(in);
          return d;
        }
      }
    }
    return super.readRecord(old, expected, in);
  }

  @Override
  protected void readField(Object record, Schema.Field field, Object oldDatum, ResolvingDecoder in, Object state)
      throws IOException {
    if (record instanceof SpecificRecordBase) {
      Conversion<?> conversion = ((SpecificRecordBase) record).getConversion(field.pos());

      Object datum;
      if (conversion != null) {
        datum = readWithConversion(oldDatum, field.schema(), field.schema().getLogicalType(), conversion, in);
      } else {
        datum = readWithoutConversion(oldDatum, field.schema(), in);
      }

      getData().setField(record, field.name(), field.pos(), datum);

    } else {
      super.readField(record, field, oldDatum, in, state);
    }
  }
}
