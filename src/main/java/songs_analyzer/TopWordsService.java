package songs_analyzer;

import org.apache.spark.api.java.JavaRDD;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import scala.Serializable;
import scala.Tuple2;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
@Service
public class TopWordsService{

    @Autowired
    private UserConf userConf;

    public List<String> topX(JavaRDD<String> lines, int x) {

        return lines.map(String::toLowerCase)
                .flatMap(WordsUtil::getWords)
                .filter(userConf::isNotGarbage)
                .mapToPair(word -> new Tuple2<>(word, 1))
                .reduceByKey(Integer::sum)
                .mapToPair(Tuple2::swap)
                .sortByKey(false)
                .map(Tuple2::_2)
                .take(x);

    }
}









