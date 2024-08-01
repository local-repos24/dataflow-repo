package ca.homedepot.search.testdataflow;

import ca.homedepot.search.testdataflow.data.Product;
import ca.homedepot.search.testdataflow.dofn.TransformToProductDoFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

public class PrintTxtFileDataflow {
    public static void main(String[] args) {
        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);

        PCollection<String> productData = p
                .apply(TextIO.read().from("Product.txt").withSkipHeaderLines(1));

        PCollection<Product> product = productData.apply("Transform String to Product",
                ParDo.of(new TransformToProductDoFn()))
                .setCoder(AvroCoder.of(Product.class));

        product.apply("print", ParDo.of(new DoFn<Product, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                System.out.println(c.element());
            }
        }));

        p.run().waitUntilFinish();
    }
}
