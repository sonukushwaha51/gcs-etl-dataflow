package com.gcp.labs.gcsetldataflow.transforms;

import com.gcp.labs.gcsetldataflow.singleton.CsvService;
import com.gcp.labs.gcsetldataflow.singleton.OutputFormat;
import com.google.inject.Inject;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import java.util.Map;

public class CsvTransformDoFn extends DoFn<OutputFormat, String> {

    private final CsvService csvService;

    @Inject
    public CsvTransformDoFn(CsvService csvService) {
        this.csvService = csvService;
    }

    @ProcessElement
    public void processElement(DoFn<OutputFormat, String>.ProcessContext context, BoundedWindow boundedWindow) {
        OutputFormat outputFormat = context.element();
        assert outputFormat != null;
        context.output(csvService.writeToCsv(outputFormat));
    }
}
