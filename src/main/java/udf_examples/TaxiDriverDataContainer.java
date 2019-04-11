package udf_examples;

import org.fluttercode.datafactory.impl.DataFactory;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.IntStream;

/**
 * @author Evgeny Borisov
 */
@Component
public class TaxiDriverDataContainer implements Serializable {
    private Map<Integer, String> id2Name = new HashMap<>();

    @PostConstruct
    public void init() {
        DataFactory dataFactory = new DataFactory();
        IntStream.iterate(1,i->i+1).limit(200).forEach(id->id2Name.put(id,dataFactory.getName()));


      /*  id2Name.put(101, "Jeka");
        id2Name.put(113, "Jonathan");
        id2Name.put(119, "Vasya");
        id2Name.put(118, "Shlomo");
        id2Name.put(117, "Yudit");
        id2Name.put(104, "Michael");
        id2Name.put(114, "John Snow");
        id2Name.put(112, "John Snow");*/
    }

    public String getById(int id) {
        return id2Name.getOrDefault(id, "John Snow");
    }







}




