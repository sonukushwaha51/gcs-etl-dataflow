package com.gcp.labs.gcsetldataflow.singleton;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
public class OutputFormat {

    public OutputFormat() {
    }

    private String name;

    private Long count;

    public String getName() {
        return name;
    }

    public Long getCount() {
        return count;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setCount(Long count) {
        this.count = count;
    }

    public static final Schema SCHEMA = SchemaBuilder.record(OutputFormat.class.getSimpleName())
            .namespace(OutputFormat.class.getPackageName())
            .fields()
            .name("name").type().stringType().noDefault()
            .name("count").type().longType().longDefault(0)
            .endRecord();
}
