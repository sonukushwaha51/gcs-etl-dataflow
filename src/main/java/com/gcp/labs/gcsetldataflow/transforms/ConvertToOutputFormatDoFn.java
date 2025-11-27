package com.gcp.labs.gcsetldataflow.transforms;

import com.gcp.labs.gcsetldataflow.singleton.OutputFormat;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;
import org.apache.beam.sdk.values.KV;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;

import static com.gcp.labs.gcsetldataflow.tags.GcsDataflowTupleTags.FAILURE_TAG;
import static com.gcp.labs.gcsetldataflow.tags.GcsDataflowTupleTags.OUTPUT_SUCCESS_TAG;

public class ConvertToOutputFormatDoFn extends DoFn<KV<String, Long>, OutputFormat> {

    private static final Logger LOGGER = LoggerFactory.getLogger(ConvertToOutputFormatDoFn.class);

    @ProcessElement
    public void processElement(DoFn<KV<String, Long>, OutputFormat>.ProcessContext context, BoundedWindow boundedWindow) {
        LOGGER.info("Starting element count");
        try {
            KV<String, Long> processedContent = context.element();
            OutputFormat outputFormat = new OutputFormat();

            assert processedContent != null;
            outputFormat.setName(processedContent.getKey());
            outputFormat.setCount(Objects.requireNonNull(processedContent.getValue()).describeConstable().orElse(0L));
            context.output(OUTPUT_SUCCESS_TAG, outputFormat);
        } catch (Exception exception) {
            LOGGER.error("Exception occurred while converting to output format: ",exception);
            context.output(FAILURE_TAG, exception.getMessage());
        }
    }

}
