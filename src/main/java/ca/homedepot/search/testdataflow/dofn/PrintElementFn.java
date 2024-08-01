package ca.homedepot.search.testdataflow.dofn;

import org.apache.beam.sdk.transforms.DoFn;

public class PrintElementFn extends DoFn<String,Void>{
    @DoFn.ProcessElement
    public void processElement(@Element String input){
    System.out.println("element:");
        System.out.println(input);
    }
}