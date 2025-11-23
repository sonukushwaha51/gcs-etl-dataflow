package com.gcp.labs.gcsetldataflow.singleton.supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gcp.labs.gcsetldataflow.singleton.SingletonResource;
import com.google.inject.Inject;

public class ObjectMapperSingletonResource extends SingletonResource<ObjectMapper> {

    @Inject
    public ObjectMapperSingletonResource(ObjectMapperSupplier objectMapperSupplier) {
        super(objectMapperSupplier);
    }

    @Override
    public Class<ObjectMapper> getRecourceClass() {
        return ObjectMapper.class;
    }
}
