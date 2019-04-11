package songs_analyzer;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.Serializable;
import java.util.List;

import static java.util.Arrays.asList;

/**
 * @author Evgeny Borisov
 */
@Component
public class UserConf implements Serializable {
    @Getter
    private List<String> garbageWords;

    @Value("${garbage}")
    public void setGarbageWords(String[] garbageArr) {
        this.garbageWords = asList(garbageArr);
    }

    public boolean isNotGarbage(String word) {
        return !garbageWords.contains(word);
    }

}
