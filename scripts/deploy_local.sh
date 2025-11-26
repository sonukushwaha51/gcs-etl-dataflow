#!/usr/bin/env bash

mvn -f ../pom.xml clean install exec:java \
-DskipTests \
-Dexec.mainClass=com.gcp.labs.gcsetldataflow.Dataflow \
-Dexec.args=" \
      --project=eighth-saga-474816-a6 \
      --inputGcsBucket=gs://text-etl-input-bucket/* \
      --outputGcsBucketSuccess=gs://csv-etl-output-bucket/success/ \
      --outputGcsBucketFailure=gs://csv-etl-output-bucket/error/ \
      --runner=DirectRunner \
      --region=us-central1 \
      --tempLocation=gs://dataflow-bucket-quickstart/gcs-etl-dataflow/temp \
      --stagingLocation=gs://dataflow-bucket-quickstart/gcs-etl-dataflow/staging
"