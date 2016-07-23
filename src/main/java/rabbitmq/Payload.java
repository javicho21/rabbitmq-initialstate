package rabbitmq;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A payload from RabbitMQ.
 */
public class Payload {

    /**
     * Metric.
     */
    private String metric = "";
    
    /**
     * Timestamp value.
     */
    private long timestampValue = -1;
    
    /**
     * Timestamp unit.
     */
    private TimeUnit timestampUnit = null;

    /**
     * Tags.
     */
    private final Map<String, String> tags = new HashMap<>();

    /**
     * Fields.
     */
    private final Map<String, Object> fields = new HashMap<>();

    /**
     * Payload builder.
     */
    public static class Builder {

        /**
         * Payload being constructed.
         */
        private final Payload internal;

        /**
         * New builder.
         */
        public Builder() {
            internal = new Payload();
        }

        /**
         * Sets metric.
         *
         * @param metric metric
         * @return this builder
         */
        public Builder setMetric(String metric) {
            internal.metric = metric;
            return this;
        }
        
        /**
         * Sets timestamp.
         * 
         * @param timestampValue timestamp
         * @param timestampUnit time unit
         * @return this builder
         */
        public Builder setTimestamp(long timestampValue, TimeUnit timestampUnit) {
            internal.timestampValue = timestampValue;
            internal.timestampUnit = timestampUnit;
            return this;
        }

        /**
         * Adds tag.
         *
         * @param key key
         * @param value value
         * @return this builder
         */
        public Builder addTag(String key, String value) {
            internal.tags.put(key, value);
            return this;
        }

        /**
         * Adds data.
         *
         * @param key key
         * @param value value
         * @return this builder
         */
        public Builder addData(String key, String value) {
            internal.fields.put(key, value);
            return this;
        }

        /**
         * Builds payload.
         *
         * @return payload
         */
        public Payload build() {
            if (internal.timestampUnit == null) {
                internal.timestampValue = System.currentTimeMillis();
                internal.timestampUnit = TimeUnit.MILLISECONDS;
            }
            return internal;
        }
    }

    /**
     * New payload.
     */
    private Payload() {

    }

    /**
     * @return the metric
     */
    public String getMetric() {
        return metric;
    }
    
    /**
     * @return timestamp value
     */
    public long getTimestampValue() {
        return timestampValue;
    }
    
    /**
     * @return timestamp unit
     */
    public TimeUnit getTimestampUnit() {
        return timestampUnit;
    }

    /**
     * @return the tags
     */
    public Map<String, String> getTags() {
        return Collections.unmodifiableMap(tags);
    }
    
    /**
     * Gets value of tag.
     * 
     * @param key key
     * @return value
     */
    public String getTag(String key) {
        return tags.get(key);
    }

    /**
     * @return the fields
     */
    public Map<String, Object> getFields() {
        return Collections.unmodifiableMap(fields);
    }
    
    /**
     * Gets value of field.
     * 
     * @param key key
     * @return value
     */
    public Object getDatum(String key) {
        return fields.get(key);
    }
    
    @Override
    public String toString() {
        return String.format("[Payload:%n"
            + "metric=%s%n"
            + "tags=%s%n"
            + "fields=%s%n"
            + "timestamp=%s %s]",
            metric, tags, fields, timestampValue, timestampUnit);
    }
}
