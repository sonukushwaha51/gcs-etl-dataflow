package com.gcp.labs.gcsetldataflow.singleton;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

public class SingletonHolder implements Serializable {

    public final ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();

    public static final SingletonHolder INSTANCE = new SingletonHolder();

    public <T> T getInstance(String className, SerializableSupplier<T> supplier, Class<T> resourceClass) {
        if (!map.containsKey(className)) {
            synchronized (this) {
                map.put(className, resourceClass);
                return supplier.get();
            }
        }
        return resourceClass.cast(map.get(className));
    }
}
