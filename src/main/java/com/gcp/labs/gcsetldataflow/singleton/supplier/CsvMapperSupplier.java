package com.gcp.labs.gcsetldataflow.singleton.supplier;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.gcp.labs.gcsetldataflow.singleton.SerializableSupplier;

public class CsvMapperSupplier implements SerializableSupplier<CsvMapper> {
    @Override
    public CsvMapper get() {
        return new CsvMapper();
    }
}
