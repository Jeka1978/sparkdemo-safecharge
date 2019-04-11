package songs_analyzer;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @author Evgeny Borisov
 */

@Service
public class MusicJudge {
    @Autowired
    private JavaSparkContext sc;
    @Autowired
    private TopWordsService wordsService;


    public List<String> topWordsOfArtist(String artistName, int x) {
        JavaRDD<String> rdd = sc.textFile("data/songs/" + artistName + "/*");
        return wordsService.topX(rdd, x);
    }







}
