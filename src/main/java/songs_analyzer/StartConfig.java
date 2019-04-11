package songs_analyzer;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.broadcast.Broadcast;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;

/**
 * @author Evgeny Borisov
 */
@Configuration
@ComponentScan
@PropertySource("classpath:user.properties")
public class StartConfig {
    @Autowired
    private SparkConf conf;



    @Bean
    public JavaSparkContext sc(){
        return new JavaSparkContext(conf);
    }

    @Bean
    public Broadcast<UserConf> userConfBroadcasted(UserConf userConf){
        return sc().broadcast(userConf);
    }
}








