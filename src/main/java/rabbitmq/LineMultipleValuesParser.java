package rabbitmq;

import java.text.ParseException;
import java.util.concurrent.TimeUnit;

/**
 * Parser that accepts InfluxDB protocol with multiple values. Assumptions:<br>
 * - Metric does not contain ,<br>
 * - Tag/field names/values do not contain , or =<br>
 *
 * @author aaronzhang
 */
public class LineMultipleValuesParser implements RabbitMQParser {

    /**
     * Whether to verify timestamp unit.
     */
    private final boolean verifyTimestamp;

    /**
     * Instantiates parser and specifies whether to verify timestamp unit. If
     * not, the timestamp is assumed to be in nanoseconds.
     *
     * @param verifyTimestamp whether to verify timestamp unit
     */
    public LineMultipleValuesParser(boolean verifyTimestamp) {
        this.verifyTimestamp = verifyTimestamp;
    }
    
    /**
     * Instantiates a parser that assumes the timestamp is in nanoseconds.
     */
    public LineMultipleValuesParser() {
        this(false);
    }

    @Override
    public Payload parse(String str) throws ParseException {
        Payload.Builder builder = new Payload.Builder();
        str = str.trim();
        int start = 0;
        int end = 0;
        char c;

        // Read metric
        try {
            while (str.charAt(end) != ',') {
                end++;
            }
        } catch (IndexOutOfBoundsException e) {
            throw new ParseException(String.format(
                "reached end of line while parsing metric in payload: %s", str),
                end);
        }
        String metric = str.substring(start, end);
        builder.setMetric(metric);

        // Read tags
        start = ++end;
        try {
            readTags:
            while (true) {
                // Read until =
                while (str.charAt(end) != '=') {
                    end++;
                }
                String tagKey = str.substring(start, end);
                // Read until , unless = is reached first
                start = ++end;
                int lastSpace = -1;
                while ((c = str.charAt(end)) != ',') {
                    if (c == '=') {
                        // Backtrack to the last space
                        if (lastSpace == -1) {
                            throw new ParseException(String.format("there must "
                                + "be a space between tags and fields in "
                                + "payload: %s", str), end);
                        }
                        end = lastSpace;
                        String tagValue = str.substring(start, end);
                        builder.addTag(tagKey, tagValue);
                        break readTags;
                    } else if (c == ' ') {
                        lastSpace = end;
                    }
                    end++;
                }
                String tagValue = str.substring(start, end);
                builder.addTag(tagKey, tagValue);
                start = ++end;
            }
        } catch (IndexOutOfBoundsException e) {
            throw new ParseException(String.format(
                "error parsing tags in payload: %s", str), end);
        }

        // Read timestamp
        int spaceBeforeTimestamp = str.lastIndexOf(" ");
        try {
            String timestampStr = str.substring(spaceBeforeTimestamp + 1);
            long timestamp = Long.parseLong(timestampStr);
            if (verifyTimestamp) {
                builder.setTimestamp(
                    timestamp, RabbitMQParser.getTimeUnit(timestampStr));
            } else {
                builder.setTimestamp(timestamp, TimeUnit.NANOSECONDS);
            }
        } catch (IndexOutOfBoundsException e) {
            throw new ParseException(String.format(
                "missing space before timestamp in payload: %s", str), 0);
        } catch (NumberFormatException e) {
            throw new ParseException(String.format(
                "timestamp not a number in payload: %s", str),
                spaceBeforeTimestamp + 1);
        }

        // Read values
        try {
            ++end;
            String[] values =
                str.substring(end, spaceBeforeTimestamp).split(",");
            for (String value : values) {
                String[] nameAndData = value.split("=");
                if (nameAndData.length != 2) {
                    throw new ParseException(String.format(
                        "error parsing data %s in payload:%n%s", value, str), 0);
                }
                builder.addData(nameAndData[0], nameAndData[1]);
            }
        } catch (IndexOutOfBoundsException e) {
            throw new ParseException(String.format(
                "error parsing values in payload: %s", str), end);
        }
        
        return builder.build();
    }
}
