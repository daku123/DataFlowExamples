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

public class CrudOperationOnfile {

    final static TupleTag<String> filterNewRecordsTag = new TupleTag<String>(){};
    final static TupleTag<String> filterUpdateRecordsTag = new TupleTag<String>(){};
    final static TupleTag<String> filterDeletedRecordsTag = new TupleTag<String>(){};

    final static String KEY_COLUMN = "ID";

    static class FilterDoFnClass extends DoFn<String,String>{

        @ProcessElement
        public void getFilterRecords(@Element String records,MultiOutputReceiver receiver){
            if (Pattern.compile("Header_change_oper.*I\"").matcher(records).find()){
                receiver.get(filterNewRecordsTag).output(records);
            }
            if (Pattern.compile("Header_change_oper.*U\"").matcher(records).find()){
                receiver.get(filterUpdateRecordsTag).output(records);
            }
            if (Pattern.compile("Header_change_oper.*D\"").matcher(records).find()){
                receiver.get(filterDeletedRecordsTag).output(records);
            }
        }
    }
    public static void main(String[] args) {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> changeFileCollection = pipeline
                                                   .apply("readLines", TextIO.read().from("D:\\sampleData\\sampleInputJsonFile.json"));

        PCollectionView<String> collectionView = pipeline.apply(Create.of(KEY_COLUMN)).apply(View.asSingleton());

        PCollection<String> baseFileCollection = pipeline.apply("readLines", TextIO.read().from("D:\\sampleData\\baseFile.json"));


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

        // filtering value with header I and U and D.
        PCollectionTuple filteredRecords = changeFileCollection.apply(ParDo.of(new FilterDoFnClass())
                                          .withOutputTags(filterNewRecordsTag, TupleTagList.of(filterUpdateRecordsTag).and(filterDeletedRecordsTag)));

        PCollection<KV<String,String>> changeFileInKVFormat =
                filteredRecords.get(filterUpdateRecordsTag).apply(
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


        PCollection<KV<String,String>> changeFileInKVFormatForDeletedRecord =
                filteredRecords.get(filterDeletedRecordsTag).apply(
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

        PCollection<KV<String, KV<String, String>>> fullOuterJoin = Join.fullOuterJoin(baseFileInKVFormat,changeFileInKVFormat,"NULL","NULL");


        /*joinedData.apply(MapElements
                         .into(TypeDescriptors.strings())
                         .via((KV<String,KV<String,String>> records)-> records.getKey()+" --> "+records.getValue().getValue()))
                .apply(TextIO.write().to("D:\\sampleData\\joinedData").withSuffix(".txt").withNumShards(1));*/

        PCollection<KV<String,KV<String,String>>> joinedForDeletedRecord = Join.leftOuterJoin(baseFileInKVFormat,changeFileInKVFormatForDeletedRecord,"D");

        joinedForDeletedRecord.apply(MapElements
                .into(TypeDescriptors.strings())
                .via((KV<String,KV<String,String>> records)-> {
                    if (records.getValue().getValue().equalsIgnoreCase("D"))
                        return records.getValue().getKey();
                   else return "";}))
                .apply(TextIO.write().to("D:\\sampleData\\joinedData").withSuffix(".txt").withNumShards(1));



        // Writing newRecords to the baseFileCollection
       // PCollectionList<String> collectionList = PCollectionList.of(baseFileCollection).and(newRecords);
        //PCollection<String> mergedWithFlatten = collectionList.apply(Flatten.<String>pCollections());

        // PCollection<String> joinedDataSetForUpdate = updateRecordsList.apply()
        //mergedWithFlatten.apply(TextIO.write().to("D:\\sampleData\\newData").withSuffix(".json").withNumShards(1));
       // filteredRecords.get(filterNewRecordsTag).apply(TextIO.write().to("D:\\sampleData\\newData").withNumShards(1).withSuffix(".json"));
        //filteredRecords.get(filterUpdateRecordsTag).apply(TextIO.write().to("D:\\sampleData\\update").withNumShards(1).withSuffix(".json"));


        pipeline.run().waitUntilFinish();
    }
}
