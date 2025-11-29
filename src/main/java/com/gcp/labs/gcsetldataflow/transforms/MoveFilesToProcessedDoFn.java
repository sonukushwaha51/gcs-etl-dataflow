package com.gcp.labs.gcsetldataflow.transforms;

import org.apache.beam.sdk.io.FileSystems;
import org.apache.beam.sdk.io.fs.ResourceId;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.windowing.BoundedWindow;

import java.io.IOException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;

import static org.apache.beam.sdk.io.fs.ResolveOptions.StandardResolveOptions.RESOLVE_FILE;

public class MoveFilesToProcessedDoFn extends DoFn<String, Void> {

    @ProcessElement
    public void processElement(DoFn<String, Void>.ProcessContext context, BoundedWindow window) {

        String filePath = context.element();
        assert filePath != null;

        ResourceId inputResourceId = FileSystems.matchNewResource(filePath, false);

        String fileName = inputResourceId.getFilename();
        ResourceId parentDirectory = inputResourceId.getCurrentDirectory();

        // 3. Define new file name and resource ID
        String newFileName = fileName.substring(0, fileName.lastIndexOf(".")) + "-processed-" + ZonedDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmss"))
                + fileName.substring(fileName.lastIndexOf("."));
        ResourceId destinationDirectory = FileSystems.matchNewResource(parentDirectory + "processed/", true);

        ResourceId destinationResourceId = destinationDirectory.resolve(newFileName, RESOLVE_FILE);
        try {
            FileSystems.rename(
                    Collections.singletonList(inputResourceId),
                    Collections.singletonList(destinationResourceId)
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
