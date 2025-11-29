package com.gcp.labs.gcsetldataflow.transforms;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

import static com.gcp.labs.gcsetldataflow.tags.GcsDataflowTupleTags.FAILURE_TAG;
import static com.gcp.labs.gcsetldataflow.tags.GcsDataflowTupleTags.SUCCESS_TAG;

public class RemoveWhiteSpaceDoFn extends DoFn<String, String> {

    private static final Logger LOGGER = LoggerFactory.getLogger(RemoveWhiteSpaceDoFn.class);

    public static final List<String> NORMALIZE_TEXTS = List.of("a", "an", "the", "of", "then", "because", "by", "in", "at");

    @ProcessElement
    public void processElement(DoFn<String, String>.ProcessContext context, BoundedWindow boundedWindow) {

        String textContent = context.element();
        String failureMessage = "Empty Text was passed";
        if (textContent != null && !textContent.isEmpty()) {
            try {
                String[] normalizedArray = Arrays.stream(textContent.split(" "))
                        .filter(text -> !NORMALIZE_TEXTS.contains(text.toLowerCase()))
                        .map(text -> text.replaceAll("\\W",""))
                        .toArray(String[]::new);
                for (String normalizedText : normalizedArray) {
                    context.output(SUCCESS_TAG, normalizedText);
                }
            } catch (Exception exception) {
                LOGGER.error("Exception while removing whitespace:",exception);
                context.output(FAILURE_TAG, exception.getMessage());
            }
        } else {
            LOGGER.error("Exception while removing whitespace: text was empty");
            context.output(FAILURE_TAG, failureMessage);
        }

    }

}
