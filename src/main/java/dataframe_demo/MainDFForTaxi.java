package dataframe_demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * @author Evgeny Borisov
 */
public class MainDFForTaxi {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("linked in");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        JavaRDD<String> rdd = sc.textFile("data/taxi_order.txt");
        JavaRDD<Row> rowRdd = rdd.map(String::toLowerCase)
                .map(line -> line.split(" "))
                .map(arr -> RowFactory.create(arr[0], arr[1], Integer.parseInt(arr[2])));

        StructType schema = DataTypes.createStructType(new StructField[]{
                        DataTypes.createStructField("id", DataTypes.StringType, false),
                        DataTypes.createStructField("city", DataTypes.StringType, false),
                        DataTypes.createStructField("km", DataTypes.IntegerType, false)
                }
        );
        DataFrame dataFrame = sqlContext.createDataFrame(rowRdd, schema);
        dataFrame.show();


    }
}
