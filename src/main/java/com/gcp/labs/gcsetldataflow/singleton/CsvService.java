package com.gcp.labs.gcsetldataflow.singleton;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.gcp.labs.gcsetldataflow.singleton.supplier.CsvMapperSingletonResource;
import com.google.inject.Inject;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;

@Slf4j
public class CsvService implements Serializable {

    private final CsvMapperSingletonResource csvMapperSingletonResource;

    private final ConcurrentHashMap<Class<?>, ObjectWriter> csvWriterMap;

    @Inject
    public CsvService(CsvMapperSingletonResource csvMapperSingletonResource, ConcurrentHashMap<Class<?>, ObjectWriter> csvWriterMap) {
        this.csvMapperSingletonResource = csvMapperSingletonResource;
        this.csvWriterMap = csvWriterMap;
    }

    public ObjectWriter getCsvWriter(Class<?> classType) {
        if (!csvWriterMap.containsKey(classType)) {
            return csvWriterMap.get(classType);
        } else {
            synchronized (this) {
              CsvMapper csvMapper = csvMapperSingletonResource.getResource();
              CsvSchema csvSchema = csvMapper.schemaFor(classType);
              ObjectWriter objectWriter = csvMapper.writer(csvSchema);
              csvWriterMap.put(classType, objectWriter);
              return objectWriter;
            }
        }
    }

    public String getCsvHeader(Class<?> classType) {
        CsvMapper csvMapper = csvMapperSingletonResource.getResource();
        CsvSchema schema = csvMapper.schemaFor(classType).withHeader();
        try {
            return csvMapper.writer(schema).writeValueAsString(null);
        } catch (JsonProcessingException e) {
            log.error("Exception occurred while writing header", e);
            throw new RuntimeException(e);
        }
    }

    public String writeToCsv(Object object) {
        ObjectWriter objectWriter = getCsvWriter(OutputFormat.class);
        try {
            return objectWriter.writeValueAsString(object);
        } catch (JsonProcessingException e) {
            log.error("Exception occurred while writing", e);
            throw new RuntimeException(e);
        }
    }

}
