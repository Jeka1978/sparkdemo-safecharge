package udf_examples;

import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

import java.util.Collection;

/**
 * @author Evgeny Borisov
 */
@Component
public class UhfRegistrarApplicationListener {

    @Autowired
    private ApplicationContext context;

    @Autowired
    private SQLContext sqlContext;

    @EventListener(ContextRefreshedEvent.class)
    public void registerAllUdfFunctions() {
        Collection<Object> udfFunctions = context.getBeansWithAnnotation(RegisterUdf.class).values();
        for (Object udfFunction : udfFunctions) {
            sqlContext.udf().register(udfFunction.getClass().getName(), (UDF1<?, ?>) udfFunction, DataTypes.StringType);

        }

    }

}




