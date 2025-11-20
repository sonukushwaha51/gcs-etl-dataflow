package com.gcp.labs.gcsetldataflow;

import com.gcp.labs.gcsetldataflow.options.GCSDataflowPipelineOptions;
import com.gcp.labs.gcsetldataflow.transforms.CountElementsDoFn;
import com.gcp.labs.gcsetldataflow.transforms.RemoveWhiteSpaceDoFn;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;

public class Dataflow {

    public static void main(String[] args) {

        Injector injector = Guice.createInjector();

        GCSDataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSDataflowPipelineOptions.class);

        String inputGcsBucket = pipelineOptions.getInputGcsBucket();
        String outputGcsBucket = pipelineOptions.getOutputGcsBucket();

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        // Add your pipeline transformations here using inputGcsBucket and outputGcsBucket
        pipeline
                .apply("Read from GCS", TextIO.read().from(inputGcsBucket))
                .apply("Process Text Data", injector.getInstance(RemoveWhiteSpaceDoFn.class))
                .apply("Count element", injector.getInstance(CountElementsDoFn.class))
                .apply("Write to GCS", TextIO.write().to(outputGcsBucket));
    }
}
