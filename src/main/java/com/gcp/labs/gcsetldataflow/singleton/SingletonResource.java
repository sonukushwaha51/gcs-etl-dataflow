package com.gcp.labs.gcsetldataflow.singleton;

import com.google.inject.Inject;

import java.io.Serializable;

public abstract class SingletonResource<T> implements Serializable {

    private final SerializableSupplier<T> supplier;


    @Inject
    public SingletonResource(SerializableSupplier<T> supplier) {
        this.supplier = supplier;
    }

    public abstract Class<T> getRecourceClass();

    public T getResource() {
        return SingletonHolder.INSTANCE.getInstance(getRecourceClass().getName(), supplier, getRecourceClass());
    }
}
