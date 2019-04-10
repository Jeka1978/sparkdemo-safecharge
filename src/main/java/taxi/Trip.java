package taxi;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @author Evgeny Borisov
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Trip {
    private String id;
    private String city;
    private int km;

    public static Trip convertLineToTrip(String line) {
        String[] arr = line.split(" ");
        return new Trip(arr[0], arr[1].toLowerCase(), Integer.parseInt(arr[2]));
    }










}
