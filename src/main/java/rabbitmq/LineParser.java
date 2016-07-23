package rabbitmq;

import java.text.ParseException;

/**
 * Parser that accepts modified InfluxDB line protocol.
 *
 * @author aaronzhang
 */
public class LineParser implements RabbitMQParser {
    @Override
    public Payload parse(String str) throws ParseException {
        Payload.Builder builder = new Payload.Builder();
        String[] pieces = splitString(str);
        // Get metric and tags
        String[] metricAndTags = pieces[0].split(",");
        if (metricAndTags.length == 0) {
            throw new ParseException(String.format(
                "no metric in payload:%n%s", str), 0);
        }
        builder.setMetric(metricAndTags[0]);
        for (int i = 1; i < metricAndTags.length; i++) {
            String[] tagAndValue = metricAndTags[i].split("=");
            if (tagAndValue.length != 2) {
                throw new ParseException(String.format(
                    "error parsing tag %s in payload:%n%s",
                    metricAndTags[i], str), 0);
            }
            builder.addTag(tagAndValue[0], tagAndValue[1]);
        }
        // Get value
        String[] values = pieces[1].split(",");
        for (String value : values) {
            String[] nameAndData = value.split("=");
            if (nameAndData.length != 2) {
                throw new ParseException(String.format(
                    "error parsing data %s in payload:%n%s", value, str), 0);
            }
            builder.addData(nameAndData[0], nameAndData[1]);
        }
        // Get timestamp
        try {
            builder.setTimestamp(Long.parseLong(pieces[2]),
                RabbitMQParser.getTimeUnit(pieces[2]));
        } catch (NumberFormatException e) {
            throw new ParseException(String.format(
                "timestamp %s could not be parsed in payload:%n%s",
                pieces[2], str), 0);
        }
        return builder.build();
    }
    
    /**
     * Splits the payload string into three sections: metric/tags, value,
     * timestamp.
     * 
     * @param str payload string
     * @return pieces of payload string
     */
    private static String[] splitString(String str) {
        str = str.split("\n")[0].trim();
        String[] pieces = new String[3];
        int index = str.length() - 1;
        boolean readingSpace = false;
        while (!readingSpace) {
            readingSpace = str.charAt(--index) == ' ';
        }
        // We've found the timestamp
        pieces[2] = str.substring(index + 1);
        while (readingSpace) {
            readingSpace = str.charAt(--index) == ' ';
        }
        int endValueSubstring = index + 1;
        index = str.lastIndexOf("value=");
        // We've found the value
        pieces[1] = str.substring(index, endValueSubstring);
        pieces[0] = str.substring(0, index).trim();
        return pieces;
    }
}
