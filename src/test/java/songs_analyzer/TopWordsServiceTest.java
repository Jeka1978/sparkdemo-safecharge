package songs_analyzer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.List;

import static java.util.Arrays.asList;
import static org.junit.Assert.*;

/**
 * @author Evgeny Borisov
 */
@RunWith(SpringRunner.class)
@ContextConfiguration(classes =StartConfig.class )
@ActiveProfiles("DEV")
public class TopWordsServiceTest {
    @Autowired
    private TopWordsService wordsService;

    @Autowired
    private JavaSparkContext sc;

    @Test
    public void topX() {
        JavaRDD<String> rdd = sc.parallelize(asList("java java java", "scala groovy scala", "java scala"));
        List<String> list = wordsService.topX(rdd, 1);
        Assert.assertEquals(1,list.size());
        Assert.assertEquals("java",list.get(0));  Assert.assertEquals("java",list.get(0));
    }
}






