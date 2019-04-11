package dataframe_demo;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.functions;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

/**
 * @author Evgeny Borisov
 */
public class Main {
    private static final String SALARY = "salary";
    private static final String AGE = "age";
    private static final String KEYWORDS = "keywords";
    private static final String KEYWORD = "keyword";
    private static final String AMOUNT = "amount";

    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("linked in");
        JavaSparkContext sc = new JavaSparkContext(conf);
        SQLContext sqlContext = new SQLContext(sc);
        DataFrame dataFrame = sqlContext.read().json("data/linkedIn/*");
       /* dataFrame.registerTempTable("linkedin");
        DataFrame dataFrameOfOldPeople = sqlContext.sql("select * from linkedin where age>40");*/
       dataFrame.show();
       dataFrame.printSchema();

        Arrays.stream(dataFrame.schema().fields()).forEach(column -> {
                    System.out.println(column.name());
                    System.out.println(column.dataType());
                }

        );

        DataFrame withSalaryDF = dataFrame.withColumn(SALARY, col(AGE).multiply(10).multiply(size(col(KEYWORDS))));
        withSalaryDF.show();

        DataFrame keywordDF = withSalaryDF.withColumn(KEYWORD, explode(col(KEYWORDS))).select(KEYWORD);
        Row row = keywordDF.groupBy(KEYWORD).agg(count(col(KEYWORD)).as(AMOUNT))
                .orderBy(col(AMOUNT).desc()).first();
        String mostPopular = row.getAs(KEYWORD);
        System.out.println("mostPopular = " + mostPopular);

        // where = filter
        withSalaryDF.filter(col(SALARY).leq(1200))
                .where(array_contains(col(KEYWORDS), mostPopular)).show();



    }
}









