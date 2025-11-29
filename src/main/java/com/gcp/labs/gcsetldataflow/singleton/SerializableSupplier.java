package com.gcp.labs.gcsetldataflow.singleton;

import java.io.Serializable;
import java.util.function.Supplier;

public interface SerializableSupplier<T> extends Serializable, Supplier<T> {

}
