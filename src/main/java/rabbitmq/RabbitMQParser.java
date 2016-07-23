package rabbitmq;

import java.text.ParseException;
import java.util.concurrent.TimeUnit;

/**
 * Parses strings into {@link Payload} objects.
 * 
 * @author aaronzhang
 */
@FunctionalInterface
public interface RabbitMQParser {
    /**
     * Parses a string to a payload.
     * 
     * @param str string
     * @return payload
     * @throws ParseException if error parsing string
     */
    Payload parse(String str) throws ParseException;
    
    /**
     * Gets the time unit for a timestamp string.
     * 
     * @param timestamp timestamp string
     * @return time unit
     */
    static TimeUnit getTimeUnit(String timestamp) {
        int length = timestamp.length();
        if (length <= 10) {
            return TimeUnit.SECONDS;
        } else if (length <= 13) {
            return TimeUnit.MILLISECONDS;
        } else if (length <= 16) {
            return TimeUnit.MICROSECONDS;
        } else {
            return TimeUnit.NANOSECONDS;
        }
    }
}