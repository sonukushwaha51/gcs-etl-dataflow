package com.gcp.labs.gcsetldataflow;

import com.gcp.labs.gcsetldataflow.options.GCSDataflowPipelineOptions;
import com.gcp.labs.gcsetldataflow.singleton.CsvService;
import com.gcp.labs.gcsetldataflow.singleton.OutputFormat;
import com.gcp.labs.gcsetldataflow.transforms.*;
import com.google.inject.Guice;
import com.google.inject.Injector;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.fs.EmptyMatchTreatment;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;
import org.apache.commons.lang3.StringUtils;

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

//        PCollection<String> pCollection = pipeline
//                .apply("Read from GCS", TextIO.read().from(inputGcsBucket));

        PCollectionTuple readFileTuple = pipeline.apply("Match files from GCS", FileIO.match().filepattern(inputGcsBucket)
                        .withEmptyMatchTreatment(EmptyMatchTreatment.ALLOW))
                .apply("Read all fileds with Metadata", FileIO.readMatches())
                .apply("Exclude Processed Folder", Filter.by((FileIO.ReadableFile file) ->
                                !file.getMetadata().resourceId().toString().contains("/processed/")))
                .apply("Extract file path", ParDo.of(injector.getInstance(ExtractFilePathDoFn.class))
                        .withOutputTags(SUCCESS_TAG, TupleTagList.of(FILE_PATH_TAG)));


        PCollectionTuple tuple = readFileTuple.get(SUCCESS_TAG)
                .setCoder(StringUtf8Coder.of())
                .apply("Process Text Data", ParDo.of(injector.getInstance(RemoveWhiteSpaceDoFn.class))
                        .withOutputTags(SUCCESS_TAG, TupleTagList.of(FAILURE_TAG)));

        PCollection<String> removeWhiteSpaceSuccess = tuple.get(SUCCESS_TAG);
        PCollection<String> removeWhiteSpaceFailure = tuple.get(FAILURE_TAG);

        PCollection<KV<String, Long>> countEachElement = removeWhiteSpaceSuccess
                .apply("Count each element", Count.perElement());
        PCollectionTuple convertToOutputFormatTuple = countEachElement
                .apply("Convert to Output format", ParDo.of(injector.getInstance(ConvertToOutputFormatDoFn.class))
                        .withOutputTags(OUTPUT_SUCCESS_TAG, TupleTagList.of(FAILURE_TAG)));

        convertToOutputFormatTuple.get(OUTPUT_SUCCESS_TAG)
                .apply("Apply Csv transform", ParDo.of(injector.getInstance(CsvTransformDoFn.class)))
                .apply("Write to GCS", TextIO.write()
                        .to(outputGcsBucketSuccess + "word-count-" + ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")))
                        .withoutSharding()
                        .withSuffix(".csv")
                        .withHeader(injector.getInstance(CsvService.class).getCsvHeader(OutputFormat.class)));

        readFileTuple.get(FILE_PATH_TAG)
                .setCoder(StringUtf8Coder.of())
                .apply("Read file path", Distinct.create())
                .apply("Move file to processed", ParDo.of(injector.getInstance(MoveFilesToProcessedDoFn.class)));

        PCollection<String> failureProcess = PCollectionList.of(removeWhiteSpaceFailure).and(convertToOutputFormatTuple.get(FAILURE_TAG))
                        .apply("Flatten error", Flatten.pCollections());
        failureProcess
                .apply("Check if failure exists", Filter.by(StringUtils::isNotBlank))
                .apply("Write to failure folder", TextIO.write()
                        .to(outputGcsBucketFailure + "word-count-error-" + ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss")))
                        .withoutSharding());

        pipeline.run();
    }
}
