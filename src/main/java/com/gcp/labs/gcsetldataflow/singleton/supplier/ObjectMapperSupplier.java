package com.gcp.labs.gcsetldataflow.singleton.supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.gcp.labs.gcsetldataflow.singleton.SerializableSupplier;

public class ObjectMapperSupplier implements SerializableSupplier<ObjectMapper> {
    @Override
    public ObjectMapper get() {
        return new ObjectMapper();
    }
}
