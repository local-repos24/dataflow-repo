package ca.homedepot.search.testdataflow.dofn;

import ca.homedepot.search.testdataflow.data.Product;
import org.apache.beam.sdk.transforms.DoFn;

import java.util.Arrays;

public class TransformToProductDoFn extends DoFn<String, Product> {

    @ProcessElement
    public void processElement(ProcessContext c){
        Product product = new Product();
        String[] fields = c.element().split(",");
        if(fields != null){

            product.setId(Long.parseLong(fields[0]));
            product.setCode(fields[1]);
            product.setName(fields[2]);
            product.setPrice(Arrays.asList(fields[3].split(";")));
            product.setCategories(Arrays.asList(fields[4].split(";")));
            product.setStock(fields[5]);
            product.setAvailability(Boolean.parseBoolean(fields[6]));
        }
        c.output(product);
    }
}
