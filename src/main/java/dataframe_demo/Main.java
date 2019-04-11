package dataframe_demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("linked in");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame dataFrame = sqlContext.read().json("data/linkedIn/*");
        dataFrame.registerTempTable("linkedin");
        DataFrame dataFrameOfOldPeople = sqlContext.sql("select * from linkedin where age>40");


    }
}
