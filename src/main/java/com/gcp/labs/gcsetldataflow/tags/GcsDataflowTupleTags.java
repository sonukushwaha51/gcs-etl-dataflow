package com.gcp.labs.gcsetldataflow.tags;

import org.apache.beam.sdk.values.TupleTag;

import java.util.Map;

public class GcsDataflowTupleTags {

    public static final TupleTag<String> SUCCESS_TAG = new TupleTag<>() {};

    public static final TupleTag<String> FAILURE_TAG = new TupleTag<>() {};

    public static final TupleTag<Map<String, Long>> OUTPUT_SUCCESS_TAG = new TupleTag<>() {};

}
