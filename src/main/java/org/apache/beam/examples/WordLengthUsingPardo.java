package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class WordLengthUsingPardo {

    static class ComputeWordLengthFn extends DoFn<String, String>{

        @ProcessElement
        public void processElement(@Element String lines, OutputReceiver<String> outputReceiver){

            String [] words = lines.split(",");
            for (String word:words)
            outputReceiver.output(word+" "+word.length()+"\n");
        }
    }

    public static void main(String[] args) {


        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();

        Pipeline pipeline = Pipeline.create(pipelineOptions);

        PCollection<String> lines = pipeline.apply("readLines", TextIO.read().from("D:\\sampleData\\words.txt"));

        PCollection<String> output = lines.apply(ParDo.of(new ComputeWordLengthFn()));

        output.apply(TextIO.write().to("D:\\sampleData\\pardoOutput").withSuffix(".txt"));

        pipeline.run().waitUntilFinish();
    }
}
