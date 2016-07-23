package initialstate;

import main.Log;

import java.time.Instant;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Observer;
import java.util.Timer;
import java.util.TimerTask;

import com.alonkadury.initialState.API;
import com.alonkadury.initialState.Bucket;
import com.alonkadury.initialState.Data;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.Multimap;
import java.util.Observable;
import rabbitmq.Payload;

/**
 * Publishes data to Initial State.
 *
 * @author aaronzhang
 */
public class InitialState implements Observer {

    private final String key;
    private final API account;
    private final int pointsToFlush;
    private final int millisToFlush;
    private final Log log;

    private final Map<String, Bucket> buckets = new HashMap<>();
    private final Multimap<Bucket, Data> bulk;
    private long lastPublished;

    public static class Builder {

        private String key;
        private int pointsToFlush = 1;
        private int millisToFlush = 0;
        private Log log;

        public Builder setKey(String key) {
            this.key = key;
            return this;
        }

        public Builder setPointsToFlush(int pointsToFlush) {
            this.pointsToFlush = pointsToFlush;
            return this;
        }

        public Builder setMillisToFlush(int millisToFlush) {
            this.millisToFlush = millisToFlush;
            return this;
        }

        public Builder setLog(Log log) {
            this.log = log;
            return this;
        }

        public InitialState build() {
            InitialState is =
                new InitialState(key, pointsToFlush, millisToFlush, log);
            log.initialStateCreated(is);
            return is;
        }
    }

    private InitialState(String key, int pointsToFlush, int millisToFlush,
        Log log) {
        this.key = key;
        this.pointsToFlush = pointsToFlush;
        this.millisToFlush = millisToFlush;
        this.log = log;
        account = new API(key);
        bulk = pointsToFlush > 1 ? ArrayListMultimap.create(10, pointsToFlush)
            : null;
        checkMillisToFlush();
    }

    /**
     * Creates bulk data.
     */
    private void publishBulk() {
        if (bulk != null && !bulk.isEmpty()) {
            for (Bucket bucket : bulk.keySet()) {
                Collection<Data> data = bulk.get(bucket);
                if (!data.isEmpty()) {
                    account.createBulkData(
                        bucket, data.toArray(new Data[data.size()]));
                }
            }
            log.initialStateWrote(bulk.size());
            bulk.clear();
            lastPublished = System.currentTimeMillis();
        }
    }

    /**
     * Publishes bulk data periodically.
     */
    private void checkMillisToFlush() {
        if (bulk != null && millisToFlush > 0) {
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    if (System.currentTimeMillis() - lastPublished
                        > millisToFlush) {
                        publishBulk();
                    }
                }
            }, millisToFlush, millisToFlush);
        }
    }

    /**
     * Updates with the given payload.
     *
     * @param o unused
     * @param arg payload
     */
    @Override
    public void update(Observable o, Object arg) {
        if (!(arg instanceof Payload)) {
            throw new IllegalArgumentException(
                "InitialState must be updated with payload");
        }
        Payload payload = (Payload) arg;
        // Different bucket for each value, if there are multiple values
        for (String fieldName : payload.getFields().keySet()) {
            String bucketName = String.format("%s-%s",
                payload.getMetric(), fieldName);
            Bucket bucket;
            if (buckets.containsKey(bucketName)) {
                bucket = buckets.get(bucketName);
            } else {
                bucket = new Bucket(bucketName, bucketName);
                buckets.put(bucketName, bucket);
                account.createBucket(bucket);
            }
            Data data = payloadToData(payload, fieldName);
            // If bulk publishing disabled, publish payload immediately
            if (bulk == null) {
                account.createData(bucket, data);
                log.initialStateWrote();
            } else {
                bulk.put(bucket, data);
                if (bulk.size() >= pointsToFlush) {
                    publishBulk();
                }
            }
        }
    }

    /**
     * @param payload payload
     * @param fieldName which field the data should contain
     * @return data
     */
    private static Data payloadToData(Payload payload, String fieldName) {
        StringBuilder tagBuilder = new StringBuilder();
        payload.getFields().forEach((tag, value) -> {
            tagBuilder
                .append(tag)
                .append('=')
                .append(value)
                .append(',');
        });
        String tag = tagBuilder.length() > 0
            ? tagBuilder.substring(0, tagBuilder.length() - 1) : "";
        String value = payload.getDatum(fieldName).toString();
        String timestamp;
        switch (payload.getTimestampUnit()) {
            case SECONDS:
                timestamp = Instant.ofEpochSecond(
                    payload.getTimestampValue()).toString();
                break;
            case MILLISECONDS:
                timestamp = Instant.ofEpochMilli(
                    payload.getTimestampValue()).toString();
                break;
            case MICROSECONDS:
                timestamp = Instant.ofEpochMilli(
                    payload.getTimestampValue() / 1000).toString();
                break;
            case NANOSECONDS:
                timestamp = Instant.ofEpochMilli(
                    payload.getTimestampValue() / 1_000_000).toString();
                break;
            default:
                throw new IllegalArgumentException("invalid timestamp unit");
        }
        return new Data(tag, value, timestamp);
    }

    /**
     * @return access key
     */
    public String getKey() {
        return key;
    }

    /**
     * @return number of points to flush
     */
    public int getPointsToFlush() {
        return pointsToFlush;
    }

    /**
     * @return number of milliseconds to flush
     */
    public int getMillisToFlush() {
        return millisToFlush;
    }

    /**
     * @return log
     */
    public Log getLog() {
        return log;
    }

    @Override
    public String toString() {
        return String.format("[InitialState: key=%s]", key);
    }
}
