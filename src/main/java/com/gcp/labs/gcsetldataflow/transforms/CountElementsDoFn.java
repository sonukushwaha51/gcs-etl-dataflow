package com.gcp.labs.gcsetldataflow.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import java.util.Arrays;
import java.util.Map;
import java.util.stream.Collectors;

import static com.gcp.labs.gcsetldataflow.tags.GcsDataflowTupleTags.OUTPUT_SUCCESS_TAG;

public class CountElementsDoFn extends DoFn<String, Map<String, Long>> {

    @ProcessElement
    public void processElement(DoFn<String, Map<String, Long>>.ProcessContext context, BoundedWindow boundedWindow) {
        String processedContent = context.element();
        Map<String, Long> map = Arrays.stream(processedContent.split(" ")).collect(Collectors.groupingBy(s -> s, Collectors.counting()));
        context.output(OUTPUT_SUCCESS_TAG, map);
    }

}
