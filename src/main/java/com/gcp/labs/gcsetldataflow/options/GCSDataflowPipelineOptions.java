package com.gcp.labs.gcsetldataflow.options;

import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.Validation;

public interface GCSDataflowPipelineOptions extends PipelineOptions, DataflowPipelineOptions {

    @Validation.Required
    String getInputGcsBucket();

    void setInputGcsBucket(String inputGcsBucket);

    @Validation.Required
    String getOutputGcsBucketSuccess();

    void setOutputGcsBucketSuccess(String outputGcsBucketSuccess);

    @Validation.Required
    String getOutputGcsBucketFailure();

    void setOutputGcsBucketFailure(String outputGcsBucketFailure);

}