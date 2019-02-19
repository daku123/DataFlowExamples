package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableRow;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.joinlibrary.Join;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.*;

import java.util.regex.Pattern;

public class SDCCode {

    final static TupleTag<String> filterNewRecordsTag = new TupleTag<String>(){};
    final static TupleTag<String> filterUpdateAndDeleteRecordsTag = new TupleTag<String>(){};
    final static TupleTag<String> filterOldRecordsTag = new TupleTag<String>(){};


    final static String KEY_COLUMN = "ID";


    static class FilterDoFnClass extends DoFn<KV<String, KV<String, String>>,String>{

        @ProcessElement
        public void getFilterRecords(@Element KV<String, KV<String, String>> records,MultiOutputReceiver receiver){
            if (records.getValue().getKey().equalsIgnoreCase("NULL"))
                receiver.get(filterNewRecordsTag).output(records.getValue().getValue());
            if (records.getValue().getValue().equalsIgnoreCase("NULL"))
                receiver.get(filterOldRecordsTag).output(records.getValue().getKey());
            if (!records.getValue().getValue().equalsIgnoreCase("NULL") &&
                    !records.getValue().getKey().equalsIgnoreCase("NULL"))
                receiver.get(filterUpdateAndDeleteRecordsTag).output(records.getValue().getValue());
        }
    }


    public static void main(String[] args) {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> changeFileCollection = pipeline
                .apply("readLines", TextIO.read().from("D:\\sampleData\\sampleInputJsonFile.json"));

        PCollectionView<String> collectionView = pipeline.apply(Create.of(KEY_COLUMN)).apply(View.asSingleton());

        PCollection<String> baseFileCollection = pipeline.apply("readLines", TextIO.read().from("D:\\sampleData\\baseFile.json"));

        //converting base file in KV format
        PCollection<KV<String,String>> baseFileInKVFormat =
                baseFileCollection.apply(
                        ParDo.of(new DoFn<String, KV<String,String>>() {
                            @ProcessElement
                            public void convertRecordToKV(@Element String records,OutputReceiver<KV<String,String>> receiver,ProcessContext context){
                                TableRow tableRow = new TableRow();
                                Gson gson = new Gson();
                                JsonElement element = gson.fromJson(records,JsonElement.class);
                                JsonObject object = element.getAsJsonObject();
                                tableRow.set(object.get(context.sideInput(collectionView)).toString(),records);
                                receiver.output(KV.of(object.get(context.sideInput(collectionView)).toString(),records));
                            }
                        }).withSideInputs(collectionView));


        //converting change file into KV Format

        PCollection<KV<String,String>> changeFileInKVFormat =
                changeFileCollection.apply(
                        ParDo.of(new DoFn<String, KV<String,String>>() {
                            @ProcessElement
                            public void convertRecordToKV(@Element String records,OutputReceiver<KV<String,String>> receiver,ProcessContext context){
                                Gson gson = new Gson();
                                JsonElement element = gson.fromJson(records,JsonElement.class);
                                JsonObject object = element.getAsJsonObject();
                                receiver.output(KV.of(object.get(context.sideInput(collectionView)).toString(),records));
                            }
                        }).withSideInputs(collectionView));


        PCollection<KV<String, KV<String, String>>> fullOuterJoin = Join.fullOuterJoin(baseFileInKVFormat,changeFileInKVFormat,"NULL","NULL");

        PCollectionTuple dataWithDifferentTag = fullOuterJoin.apply(ParDo.of(new FilterDoFnClass())
                                                             .withOutputTags(filterNewRecordsTag, TupleTagList.of(filterOldRecordsTag)
                                                                                                  .and(filterUpdateAndDeleteRecordsTag)));


        PCollection<String> updatedRecords = dataWithDifferentTag
                                             .get(filterUpdateAndDeleteRecordsTag)
                                             .apply(ParDo.of(new DoFn<String, String>() {
                                             @ProcessElement
                                             public void filterUpdatedRecords(@Element String records,OutputReceiver<String> receiver) {
                                                 if (Pattern.compile("Header_change_oper.*U\"").matcher(records).find()) {
                                                     receiver.output(records);
                                                 }
                                             }}));

        PCollection<String> freshNewData = dataWithDifferentTag.get(filterNewRecordsTag);
        PCollection<String> unchangedData = dataWithDifferentTag.get(filterOldRecordsTag);

         PCollectionList<String> collectionList = PCollectionList.of(freshNewData).and(unchangedData).and(updatedRecords);
         PCollection<String> mergedWithFlatten = collectionList.apply(Flatten.<String>pCollections());

         mergedWithFlatten.apply(TextIO.write().to("D:\\sampleData\\finalOutput").withSuffix(".json").withNumShards(1));

        pipeline.run().waitUntilFinish();

    }
}
