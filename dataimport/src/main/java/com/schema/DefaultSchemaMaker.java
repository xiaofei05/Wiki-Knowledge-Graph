package com.schema;

import org.janusgraph.core.Cardinality;
import org.janusgraph.core.PropertyKey;
import org.janusgraph.core.schema.PropertyKeyMaker;


public class DefaultSchemaMaker implements org.janusgraph.core.schema.DefaultSchemaMaker {
    @Override
    public Cardinality defaultPropertyCardinality(String s) {
        return Cardinality.LIST;
    }

    @Override
    public PropertyKey makePropertyKey(PropertyKeyMaker factory) {
        return factory.cardinality(this.defaultPropertyCardinality(factory.getName())).dataType(String.class).make();
    }

    @Override
    public boolean ignoreUndefinedQueryTypes() {
        return false;
    }
}
