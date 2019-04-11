package songs_analyzer;

import org.springframework.context.annotation.AnnotationConfigApplicationContext;

import java.util.List;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
        System.setProperty("spring.profiles.active", "DEV");
        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(StartConfig.class);
        MusicJudge musicJudge = context.getBean(MusicJudge.class);
//        List<String> list = musicJudge.topWordsOfArtist("beatles", 3);
        List<String> britney = musicJudge.topWordsOfArtist("britney", 6);
        List<String> ketty = musicJudge.topWordsOfArtist("ketty", 6);
        List<String> pinkFloyd = musicJudge.topWordsOfArtist("pink floyd", 6);
        System.out.println("britney = " + britney);
        System.out.println("ketty = " + ketty);
        System.out.println("pinkFloyd = " + pinkFloyd);
    }
}
