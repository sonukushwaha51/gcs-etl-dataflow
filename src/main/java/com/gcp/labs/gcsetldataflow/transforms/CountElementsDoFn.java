package com.gcp.labs.gcsetldataflow.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

public class CountElementsDoFn extends DoFn<String, String> {

    @ProcessElement
    public void processElement(DoFn<String, String>.ProcessContext context, BoundedWindow boundedWindow) {
        String processedContent = context.element();
        assert processedContent != null;
        for (String processedText : processedContent.split(" ")) {

        }
    }

}
