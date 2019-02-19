package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TypeDescriptors;

import java.util.Arrays;

import static org.apache.beam.sdk.values.TypeDescriptors.strings;

public class WordLength {
    public static void main(String[] args) {
        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> lines = pipeline.apply("readLines", TextIO.read().from("D:\\sampleData\\words.txt"));

        PCollection<String> length = lines.apply(FlatMapElements.into(strings())
                                                             .via((String words)-> Arrays.asList(words.split(","))));

        //PCollection<KV<String, Long>> countWords = length.apply(Count.perElement());
         PCollection<KV<String,Integer>> lengthOfEachWord = length.apply(MapElements
                                                                         .into(TypeDescriptors.kvs(TypeDescriptors.strings(),TypeDescriptors.integers()))
                                                                         .via((String word)->KV.of(word,word.length())));

         lengthOfEachWord.apply(MapElements.into(TypeDescriptors.strings())
                                            .via((KV<String,Integer>word)->word.getKey()+" - "+word.getValue()+"\n"))
                         .apply(TextIO.write().to("D:\\sampleData\\output").withSuffix(".txt"));

       // PCollection<String> count = lines.apply(MapElements.into(TypeDescriptors.strings()).via((String word) -> word.toUpperCase()));

        PCollection<KV<String, Long>> count = length.apply(Count.perElement());

        PCollection<String> concatWords = count.apply(MapElements.into(TypeDescriptors.strings())
                                                                 .via((KV<String,Long>word)->word.getKey()+" "+word.getValue()));

       // concatWords.apply(TextIO.write().to("D:\\sampleData\\output").withSuffix(".txt"));
        pipeline.run().waitUntilFinish();

    }
}
