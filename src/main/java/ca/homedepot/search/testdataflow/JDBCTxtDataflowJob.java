package ca.homedepot.search.testdataflow;

import ca.homedepot.search.testdataflow.data.Product;
import ca.homedepot.search.testdataflow.dofn.PrintElementFn;
import ca.homedepot.search.testdataflow.dofn.TransformToProductDoFn;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.extensions.avro.coders.AvroCoder;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.jdbc.JdbcIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PCollection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class JDBCTxtDataflowJob {
    public static void main(String[] args) {
        createTableIfDoesNotExist();

        PipelineOptions options = PipelineOptionsFactory.create();

        Pipeline p = Pipeline.create(options);

        PCollection<String> textFileData = p
                .apply("Read Txt File",TextIO.read().from("Product.txt").withSkipHeaderLines(1));

         PCollection<Product> productPCollection =textFileData
                 .apply("Transform to Product Object", ParDo.of(new TransformToProductDoFn()))
                .setCoder(AvroCoder.of(Product.class));

        productPCollection.apply(ParDo.of(new DoFn<Product, Void>() {
            @ProcessElement
            public void processElement(ProcessContext c){
                System.out.println(c.element());
            }
        }));

        productPCollection.apply("Insert into DB", JdbcIO.write()
                .withDataSourceConfiguration(JdbcIO.DataSourceConfiguration
                        .create( "com.mysql.jdbc.Driver",
                                "jdbc:mysql://hostname:3306/storeindexerdb")
                        .withUsername("root")
                        .withPassword("mysql12"))
                .withStatement("INSERT INTO ProductDF values(?,?,?,?,?,?)")
                .withPreparedStatementSetter()
        )


         p.run().waitUntilFinish();
    }

    private static void createTableIfDoesNotExist(){
        String url = "jdbc:mysql://localhost:3306/storeindexerdb";
        String user = "root";
        String password = "mysql12";

        String createTableSQL = "CREATE TABLE IF NOT EXISTS ProductDF ("
                + "id INT AUTO_INCREMENT PRIMARY KEY, "
                + "code VARCHAR(255) NULL,"
                + "name VARCHAR(255) NULL, "
                + "price TEXT NULL, "
                + "categories TEXT NULL,"
                + "stock VARCHAR(255),"
                + "availability BOOLEAN NULL)";

        try (Connection conn = DriverManager.getConnection(url, user, password);
             Statement stmt = conn.createStatement()) {
            stmt.execute(createTableSQL);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
