package ca.homedepot.search.testdataflow;


import ca.homedepot.search.testdataflow.dofn.PrintElementFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

/**
 * command to run the dataflow:
 * last updated:
 * compile exec:java -Dexec.mainClass=ca.homedepot.search.testdataflow.TestDataflowJob -Dexec.cleanupDaemonThreads=false
 *
 *
 * compile exec:java -Dexec.mainClass=ca.homedepot.search.testdataflow.TestDataflowJob -Dexec.cleanupDaemonThreads=false -D "exec.args=--project=np-ca-search --csvFile=gs://np-ca-search-intent-router/dev --runner=DirectRunner --serviceAccount==search@home-depot-339919.iam.gserviceaccount.com   --enableStreamingEngine --numWorkers=1 --redisHost=localhost --redisPort=6379 --redsTimeout=5000"
 */
public class TestDataflowJob {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();
        // Create pipeline
        Pipeline p = Pipeline.create(options);
        // Read text data from Sample.txt
        PCollection<String> textData = p
                .apply(TextIO.read().from("Sample.txt"));

        textData.apply("Print", ParDo.of(new PrintElementFn()));

        // Write to the output file with wordcounts as a prefix
//        textData.apply(TextIO.write().to("wordcounts"));

        // Run the pipeline
        p.run().waitUntilFinish();
    }
}

