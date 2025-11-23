package com.gcp.labs.gcsetldataflow.singleton.supplier;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.gcp.labs.gcsetldataflow.singleton.SingletonResource;
import com.google.inject.Inject;

public class CsvMapperSingletonResource extends SingletonResource<CsvMapper> {

    @Inject
    public CsvMapperSingletonResource(CsvMapperSupplier csvMapperSupplier) {
        super(csvMapperSupplier);
    }

    @Override
    public Class<CsvMapper> getRecourceClass() {
        return CsvMapper.class;
    }
}
