package ca.homedepot.search.testdataflow.data;

import lombok.Data;

import java.io.Serializable;
import java.util.List;

@Data
public class Product implements Serializable {
    private long id;
    private String code;
    private String name;
    private List<String> price;
    private List<String> categories;
    private String stock;
    private boolean availability;
}
