package com.gcp.labs.gcsetldataflow.transforms;

import com.gcp.labs.gcsetldataflow.singleton.CsvService;
import com.google.inject.Inject;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import java.util.Map;

public class CsvTransformDoFn extends DoFn<Map<String, Long>, String> {

    private final CsvService csvService;

    @Inject
    public CsvTransformDoFn(CsvService csvService) {
        this.csvService = csvService;
    }

    @ProcessElement
    public void processElement(DoFn<Map<String, Long>, String>.ProcessContext context, BoundedWindow boundedWindow) {
        Map<String, Long> outputFormat = context.element();
        assert outputFormat != null;
        context.output(csvService.writeToCsv(outputFormat));
    }
}
