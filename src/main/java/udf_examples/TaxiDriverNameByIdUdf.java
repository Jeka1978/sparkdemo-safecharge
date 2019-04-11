package udf_examples;

import org.apache.spark.sql.api.java.UDF1;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * @author Evgeny Borisov
 */
// id of driver this time will be an int
@RegisterUdf
public class TaxiDriverNameByIdUdf implements UDF1<Integer, String> {

    @Autowired
    private TaxiDriverDataContainer namesContainer;


    @Override
    public String call(Integer id) {
        return namesContainer.getById(id);
    }
}



