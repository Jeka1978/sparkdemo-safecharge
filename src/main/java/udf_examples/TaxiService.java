package udf_examples;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
@Service
public class TaxiService {
    public static final String NAME = "name";
    public static final String ID = "id";
    @Autowired
    private SQLContext sqlContext;
    @Autowired
    private JavaSparkContext sc;


    public void showTaxiDfAfterEtl() {
        JavaRDD<String> rdd = sc.textFile("data/taxi_order.txt");
        JavaRDD<Row> rowRdd = readDataToRdd(rdd);

        StructType schema = buildSchema();
        DataFrame dataFrame = sqlContext.createDataFrame(rowRdd, schema);

        dataFrame.withColumn(NAME, callUDF(TaxiDriverNameByIdUdf.class.getName(),col(ID))).show();

    }

    private JavaRDD<Row> readDataToRdd(JavaRDD<String> rdd) {
        return rdd.map(String::toLowerCase)
                .map(line -> line.split(" "))
                .map(arr -> RowFactory.create(Integer.parseInt(arr[0]), arr[1], Integer.parseInt(arr[2])));
    }

    private StructType buildSchema() {
        return DataTypes.createStructType(new StructField[]{
                            DataTypes.createStructField("id", DataTypes.IntegerType, false),
                            DataTypes.createStructField("city", DataTypes.StringType, false),
                            DataTypes.createStructField("km", DataTypes.IntegerType, false)
                    }
            );
    }


}
