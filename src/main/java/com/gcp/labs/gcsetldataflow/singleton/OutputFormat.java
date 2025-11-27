package com.gcp.labs.gcsetldataflow.singleton;

import lombok.Data;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;

import java.util.List;

@Data
public class OutputFormat {

    private String name;

    private long count;

    public static final Schema SCHEMA = SchemaBuilder.record("OutputEvent")
            .namespace("com.gcp.labs.gsetldataflow.singleton.outputformat")
            .fields()
            .name("name").type().stringType().noDefault()
            .name("count").type().longType().longDefault(0)
            .endRecord();
}
