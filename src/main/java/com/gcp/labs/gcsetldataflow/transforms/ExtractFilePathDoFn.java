package com.gcp.labs.gcsetldataflow.transforms;

import org.apache.beam.sdk.io.FileIO;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import static com.gcp.labs.gcsetldataflow.tags.GcsDataflowTupleTags.FILE_PATH_TAG;
import static com.gcp.labs.gcsetldataflow.tags.GcsDataflowTupleTags.SUCCESS_TAG;

public class ExtractFilePathDoFn extends DoFn<FileIO.ReadableFile, String> {

    @ProcessElement
    public void processElement(DoFn<FileIO.ReadableFile, String>.ProcessContext context, BoundedWindow boundedWindow) {
        FileIO.ReadableFile readableFile = context.element();
        assert readableFile != null;
        String filePath = readableFile.getMetadata().resourceId().toString();
        context.output(FILE_PATH_TAG, filePath);


        try {
            String fileContent = readableFile.readFullyAsUTF8String();
            if (!fileContent.trim().isEmpty()) {
                context.output(SUCCESS_TAG, fileContent);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
