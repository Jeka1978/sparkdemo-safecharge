package taxi;

import org.apache.spark.Accumulator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import static org.apache.spark.storage.StorageLevel.MEMORY_AND_DISK;

/**
 * @author Evgeny Borisov
 */
public class Main {
    public static void main(String[] args) {
        SparkConf conf = new SparkConf().setMaster("local[*]").setAppName("taxi");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaRDD<String> lines = sc.textFile("data/taxi_order.txt");
        JavaRDD<Trip> tripRdd = lines.map(Trip::convertLineToTrip);
        tripRdd.persist(MEMORY_AND_DISK());

        Accumulator<Integer> smallTripsAcc = sc.accumulator(0, "smallTripsAcc");

        tripRdd.foreach(trip ->{
            if (trip.getKm() < 5) {
               smallTripsAcc.add(1);
            }
        });
        Integer smallTrips = smallTripsAcc.value();
        System.out.println("smallTrips = " + smallTrips);


        JavaRDD<Trip> bostonTripsRdd = tripRdd.filter(trip -> trip.getCity().equals("boston"));
        bostonTripsRdd.persist(MEMORY_AND_DISK());

        long longTripsToBostonAmount = bostonTripsRdd.filter(trip -> trip.getKm() > 10).count();
        System.out.println("longTripsToBostonAmount = " + longTripsToBostonAmount);
        Double totalKmToBoston = bostonTripsRdd.mapToDouble(Trip::getKm).sum();
        System.out.println("totalKmToBoston = " + totalKmToBoston);

        JavaPairRDD<String, String> driversPairRdd = sc.textFile("data/drivers.txt")
                .map(line -> line.split(", "))
                .mapToPair(arr -> new Tuple2<>(arr[0], arr[1]));
        JavaPairRDD<String, Integer> trip2KmPairRdd = tripRdd.mapToPair(trip -> new Tuple2<>(trip.getId(), trip.getKm()))
                .reduceByKey(Integer::sum);


        JavaPairRDD<String, Tuple2<Integer, String>> joinRdd = trip2KmPairRdd.join(driversPairRdd);
        joinRdd.mapToPair(Tuple2::_2).sortByKey(false).take(3)
                .forEach(System.out::println);


    }
}









