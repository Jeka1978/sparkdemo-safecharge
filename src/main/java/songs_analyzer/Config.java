package songs_analyzer;

import org.apache.spark.SparkConf;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Profile;

/**
 * @author Evgeny Borisov
 */
@Configuration
public class Config {
    @Bean
    @Profile("PROD")
    public SparkConf sparkConf() {
        return new SparkConf().setAppName("songs analyzer");
    }
    @Bean
    @Profile("DEV")
    public SparkConf sparkConf2() {
        return new SparkConf().setAppName("songs analyzer").setMaster("local[*]");
    }
}



