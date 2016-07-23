package rabbitmq;

import java.text.ParseException;
import java.util.concurrent.TimeUnit;

/**
 * Parses RabbitMQ payload strings into {@link RabbitMQ.Payload} objects.
 * 
 * @author aaronzhang
 */
public class DefaultParser implements RabbitMQParser {
    @Override
    public Payload parse(String str) throws ParseException {
        Payload.Builder builder = new Payload.Builder();
        // Split payload into lines
        int lineNum = 0;
        try {
            String[] lines = str.split("\n");
            for (; lineNum < lines.length; lineNum++) {
                String line = lines[lineNum];
                switch (lineNum) {
                    // Line 1 is the metric
                    case 1:
                        builder.setMetric(line.split("\"")[1]);
                        break;
                    // Line 4 is the tags
                    case 4:
                        String[] line4Split = line.split("\"");
                        for (int i = 1; i < line4Split.length; i += 4) {
                            builder.addTag(line4Split[i], line4Split[i + 2]);
                        }
                        break;
                    // Line 5 is the values
                    case 5:
                        String[] line5Split = line.split("\"");
                        builder.setTimestamp(Long.parseLong(line5Split[1]),
                            TimeUnit.MILLISECONDS);
                        builder.addData("value", line5Split[3]);
                        break;
                    default:
                }
            }
            return builder.build();
        } catch (Exception e) {
            throw new ParseException("error parsing payload", lineNum);
        }
    }
}