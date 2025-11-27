package com.gcp.labs.gcsetldataflow;

import com.gcp.labs.gcsetldataflow.options.GCSDataflowPipelineOptions;
import com.gcp.labs.gcsetldataflow.singleton.CsvService;
import com.gcp.labs.gcsetldataflow.singleton.OutputFormat;
import com.gcp.labs.gcsetldataflow.transforms.ConvertToOutputFormatDoFn;
import com.gcp.labs.gcsetldataflow.transforms.CsvTransformDoFn;
import com.gcp.labs.gcsetldataflow.transforms.RemoveWhiteSpaceDoFn;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.*;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;

import static com.gcp.labs.gcsetldataflow.tags.GcsDataflowTupleTags.*;

public class Dataflow {

    public static void main(String[] args) {

        Injector injector = Guice.createInjector();

        GCSDataflowPipelineOptions pipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(GCSDataflowPipelineOptions.class);

        String inputGcsBucket = pipelineOptions.getInputGcsBucket();
        String outputGcsBucketSuccess = pipelineOptions.getOutputGcsBucketSuccess();
        String outputGcsBucketFailure = pipelineOptions.getOutputGcsBucketFailure();

        Pipeline pipeline = Pipeline.create(pipelineOptions);
        AvroCoder<OutputFormat> avroCoder = AvroCoder.of(OutputFormat.class, OutputFormat.SCHEMA);
        pipeline.getCoderRegistry().registerCoderForClass(OutputFormat.class, avroCoder);

        // Add your pipeline transformations here using inputGcsBucket and outputGcsBucket

        PCollection<String> pCollection = pipeline
                .apply("Read from GCS", TextIO.read().from(inputGcsBucket));

        PCollectionTuple tuple = pCollection
                .apply("Process Text Data", ParDo.of(injector.getInstance(RemoveWhiteSpaceDoFn.class)).withOutputTags(SUCCESS_TAG, TupleTagList.of(FAILURE_TAG)));

        PCollection<String> removeWhiteSpaceSuccess = tuple.get(SUCCESS_TAG);
        PCollection<String> removeWhiteSpaceFailure = tuple.get(FAILURE_TAG);

        PCollection<KV<String, Long>> countEachElement = removeWhiteSpaceSuccess
                .apply("Count each element", Count.perElement());
        PCollectionTuple convertToOutputFormatTuple = countEachElement
                .apply("Convert to Output format", ParDo.of(injector.getInstance(ConvertToOutputFormatDoFn.class))
                        .withOutputTags(OUTPUT_SUCCESS_TAG, TupleTagList.of(FAILURE_TAG)));

        convertToOutputFormatTuple.get(OUTPUT_SUCCESS_TAG)
                .apply("Apply Csv transform", ParDo.of(injector.getInstance(CsvTransformDoFn.class)))
                .apply("Write to GCS", TextIO.write().to(outputGcsBucketSuccess + "word-count-" + ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")))
                        .withNumShards(1)
                        .withSuffix(".csv")
                        .withHeader(injector.getInstance(CsvService.class).getCsvHeader(OutputFormat.class)));

        PCollection<String> failureProcess = PCollectionList.of(removeWhiteSpaceFailure).and(convertToOutputFormatTuple.get(FAILURE_TAG))
                        .apply("Flatten error", Flatten.pCollections());

        failureProcess
                .apply("Write to failure folder", TextIO.write().to(outputGcsBucketFailure + "word-count-error-" + ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))));

        pipeline.run();
    }
}
