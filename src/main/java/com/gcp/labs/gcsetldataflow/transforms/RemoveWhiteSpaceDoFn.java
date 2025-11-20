package com.gcp.labs.gcsetldataflow.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import java.util.Arrays;
import java.util.List;

import static com.gcp.labs.gcsetldataflow.tags.GcsDataflowTupleTags.FAILURE_TAG;
import static com.gcp.labs.gcsetldataflow.tags.GcsDataflowTupleTags.SUCCESS_TAG;

public class RemoveWhiteSpaceDoFn extends DoFn<String, String> {

    public static final List<String> NORMALIZE_TEXTS = List.of("a", "an", "the", "of", "then", "because", "by", "in", "at");

    @ProcessElement
    public void processElement(DoFn<String, String>.ProcessContext context, BoundedWindow boundedWindow) {

        String textContent = context.element();
        String finalString = null;
        String failureMessage = "Empty Text was passed";
        if (textContent != null && !textContent.isEmpty()) {
            try {
                String[] normalizedArray = (String[]) Arrays.stream(textContent.trim().split(" "))
                        .filter(text -> !NORMALIZE_TEXTS.contains(textContent.toLowerCase())).toArray();
                finalString = Arrays.stream(normalizedArray).reduce("", (a, b) -> a + b);
                context.output(SUCCESS_TAG, finalString);
            } catch (Exception exception) {
                context.output(FAILURE_TAG, exception.getMessage());
            }
        } else {
            context.output(FAILURE_TAG, failureMessage);
        }

    }

}
