package org.apache.beam.examples;

import com.google.api.services.bigquery.model.TableCell;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.options.PipelineOptions.DirectRunner;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SimpleJsonParserWithDataFlow {

static class ParsingJson extends DoFn<String,TableRow>{
    @ProcessElement
    public void parseJson(@Element String line,OutputReceiver<TableRow> receiver){

        Gson gson = new Gson();
        JsonElement element = gson.fromJson(line,JsonElement.class);
        JsonObject object = element.getAsJsonObject();

        /*TableCell tableCell = new TableCell();
        List<TableCell> listOfTableCell = new ArrayList<>();
        listOfTableCell.add(tableCell.setV(object));

        TableRow tableRow = new TableRow();
        tableRow.setF(listOfTableCell);
        receiver.output(tableRow);*/

        TableRow tableRow = new TableRow();
        for (Map.Entry<String,JsonElement> colElement:object.entrySet()){

            tableRow.set(colElement.getKey(),colElement.getValue().toString());
        }
            receiver.output(tableRow);

    }
}

public static TableSchema getSchema(){

    List<TableFieldSchema> schema = new ArrayList<>();
        schema.add(new TableFieldSchema().setName("color").setType("String").setMode("NULLABLE"));
        schema.add(new TableFieldSchema().setName("color").setType("String").setMode("NULLABLE"));
    TableSchema tableSchema = new TableSchema();
    tableSchema.setFields(schema);

    return tableSchema;
}

    public interface OptionInterface extends DataflowPipelineOptions {

        /**
         * By default, this example reads from a public dataset containing the text of King Lear. Set
         * this option to choose a different input file or glob.
         */

    }
    public static void main(String[] args) {

        PipelineOptions pipelineOptions = PipelineOptionsFactory.create();
       // PipelineOptions pipelineOptions = PipelineOptionsFactory.as(PipelineOptions.class);
        //DataflowPipelineOptions dataflowPipelineOptions = PipelineOptionsFactory.as(DataflowPipelineOptions.class);
       // DataflowPipelineOptions dataflowPipelineOptions = PipelineOptionsFactory.fromArgs(args).withValidation().as(OptionInterface.class);
        Pipeline pipeline = Pipeline.create(pipelineOptions);

        /*dataflowPipelineOptions.setTempLocation("gs://geocoding_data_av/temp/tempData");
        dataflowPipelineOptions.setRunner(DirectRunner.class);
        dataflowPipelineOptions.setProject("x-refinery-button-soup");
*/
        PCollection<String> readFile = pipeline.apply(TextIO.read().from("D:\\sampleData\\sample.json"));

       //readFile.apply(ParDo.of(new ParsingJson())).apply(TextIO.write().to("D:\\sampleData\\output").withSuffix(".json"));

       PCollection<TableRow> tableRowPCollection = readFile.apply(ParDo.of(new ParsingJson()));
       /*tableRowPCollection.apply(BigQueryIO.writeTableRows().to("FB_ERP_DATA.testBQTable").withSchema(getSchema())
               .withCreateDisposition(BigQueryIO.Write.CreateDisposition.CREATE_IF_NEEDED));
       */ pipeline.run().waitUntilFinish();
    }
}
