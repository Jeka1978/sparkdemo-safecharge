package udf_examples;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * @author Evgeny Borisov
 */
@Configuration
@ComponentScan
public class Conf {
    @Bean
    public JavaSparkContext sc(){
        SparkConf conf = new SparkConf().setAppName("taxi udf").setMaster("local[*]");
        return new JavaSparkContext(conf);
    }
    @Bean
    public SQLContext sqlContext(){
        return new SQLContext(sc().sc());
    }


}
