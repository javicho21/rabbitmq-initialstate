package main;

import initialstate.InitialState;
import rabbitmq.RabbitMQ;

import java.io.IOException;
import java.util.Date;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

/**
 * Logs messages from RabbitMQ and InitialState.
 *
 * @author aaronzhang
 */
public class Log {

    private final String normalLog;
    private final int normalLogNum;
    private final int normalLogSize;
    private final long normalLogInterval;
    private final String errorLog;
    private final int errorLogNum;
    private final int errorLogSize;

    /**
     * Logger.
     */
    private final Logger logger;

    /**
     * Number of payloads read from RabbitMQ since last normal log entry.
     */
    private final AtomicLong rabbitRead = new AtomicLong(0);

    /**
     * Number of payloads backed up to RabbitMQ since last normal log entry.
     */
    private final AtomicLong rabbitBacked = new AtomicLong(0);

    /**
     * Number of erroneous payloads from RabbitMQ since last normal log entry.
     * More detailed error messages will appear in the error log.
     */
    private final AtomicLong rabbitErrors = new AtomicLong(0);

    /**
     * Number of points written to InitialState since last normal log entry.
     */
    private final AtomicLong initialStateWrote = new AtomicLong(0);

    /**
     * Logger builder.
     */
    public static class Builder {

        private String normalLog;
        private int normalLogNum;
        private int normalLogSize;
        private long normalLogInterval;
        private String errorLog;
        private int errorLogNum;
        private int errorLogSize;

        /**
         * Instantiates builder.
         */
        public Builder() {

        }

        /**
         * Sets normal log. If not set, will not log success messages.
         *
         * @param normalLog normal log
         * @return this builder
         */
        public Builder setNormalLog(String normalLog) {
            this.normalLog = normalLog;
            return this;
        }

        /**
         * Sets maximum number of normal log files. Defaults to 1.
         *
         * @param normalLogNum
         * @return this builder
         */
        public Builder setNormalLogNum(int normalLogNum) {
            this.normalLogNum = normalLogNum;
            return this;
        }

        /**
         * Sets maximum size of each normal log file, in bytes. Defaults to
         * 1000000 (1 MB).
         *
         * @param normalLogSize
         * @return this builder
         */
        public Builder setNormalLogSize(int normalLogSize) {
            this.normalLogSize = normalLogSize;
            return this;
        }

        /**
         * Sets time, in milliseconds, between normal log entries. Defaults to
         * 5000 (5 seconds). (Error messages will be reported immediately.)
         *
         * @param normalLogInterval
         * @return this builder
         */
        public Builder setNormalLogInterval(long normalLogInterval) {
            this.normalLogInterval = normalLogInterval;
            return this;
        }

        /**
         * Sets error log. If not set, will not log error messages.
         *
         * @param errorLog error log
         * @return this builder
         */
        public Builder setErrorLog(String errorLog) {
            this.errorLog = errorLog;
            return this;
        }

        /**
         * Sets maximum number of error log files. Defaults to 1.
         *
         * @param errorLogNum
         * @return this builder
         */
        public Builder setErrorLogNum(int errorLogNum) {
            this.errorLogNum = errorLogNum;
            return this;
        }

        /**
         * Sets maximum size of each error log file, in bytes. Defaults to
         * 1000000 (1 MB).
         *
         * @param errorLogSize
         * @return this builder
         */
        public Builder setErrorLogSize(int errorLogSize) {
            this.errorLogSize = errorLogSize;
            return this;
        }

        /**
         * Builds logger.
         *
         * @return logger
         * @throws IOException if invalid normal log or error log files
         */
        public Log build() throws IOException {
            if (normalLogNum <= 0) {
                normalLogNum = 1;
            }
            if (normalLogSize <= 0) {
                normalLogSize = 1_000_000;
            }
            if (normalLogInterval <= 0) {
                normalLogInterval = 5_000;
            }
            if (errorLogNum <= 0) {
                errorLogNum = 1;
            }
            if (errorLogSize <= 0) {
                errorLogSize = 1_000_000;
            }
            Log log = new Log(
                normalLog, normalLogNum, normalLogSize, normalLogInterval,
                errorLog, errorLogNum, errorLogSize);
            log.normalLog();
            return log;
        }
    }

    /**
     * Formatter for one-line logs.
     */
    private static class OneLineFormatter extends Formatter {

        @Override
        public String format(LogRecord record) {
            return String.format("%s %s %s %s: %s%n",
                new Date(record.getMillis()),
                record.getSourceClassName(),
                record.getSourceMethodName(),
                record.getLevel(),
                record.getMessage());
        }
    }

    /**
     * Instantiates logger.
     *
     * @param normalLog
     * @param normalLogNum
     * @param normalLogSize
     * @param errorLog
     * @param errorLogNum
     * @param errorLogSize
     * @throws IOException
     */
    private Log(String normalLog, int normalLogNum, int normalLogSize,
        long normalLogInterval,
        String errorLog, int errorLogNum, int errorLogSize) throws IOException {
        this.normalLog = normalLog;
        this.normalLogNum = normalLogNum;
        this.normalLogSize = normalLogSize;
        this.normalLogInterval = normalLogInterval;
        this.errorLog = errorLog;
        this.errorLogNum = errorLogNum;
        this.errorLogSize = errorLogSize;

        // Make a new logger for each instance of this class
        logger = Logger.getLogger(super.toString());
        logger.setLevel(Level.ALL);
        if (normalLog != null) {
            FileHandler normalHandler = new FileHandler(
                normalLog, normalLogSize, normalLogNum, true);
            normalHandler.setFormatter(new OneLineFormatter());
            normalHandler.setFilter(
                l -> l.getLevel().intValue() <= Level.INFO.intValue());
            logger.addHandler(normalHandler);
        }
        if (errorLog != null) {
            FileHandler errorHandler = new FileHandler(
                errorLog, errorLogSize, errorLogNum, true);
            errorHandler.setFormatter(new OneLineFormatter());
            errorHandler.setLevel(Level.WARNING);
            logger.addHandler(errorHandler);
        }
    }

    /**
     * Indicates that RabbitMQ has been instantiated.
     *
     * @param rabbit RabbitMQ
     */
    public void rabbitCreated(RabbitMQ rabbit) {
        logger.info(String.format("RabbitMQ created: %s", oneLine(rabbit)));
    }

    /**
     * Indicates that a payload has been successfully read from RabbitMQ.
     */
    public void rabbitRead() {
        rabbitRead.incrementAndGet();
    }

    /**
     * Indicates that a payload has been successfully backed up to RabbitMQ.
     */
    public void rabbitBacked() {
        rabbitBacked.incrementAndGet();
    }

    /**
     * Indicates that a RabbitMQ payload is erroneous.
     *
     * @param payload the erroneous payload
     * @param e exception thrown from processing the payload
     * @param backed whether the erroneous message has been backed up
     */
    public void rabbitError(String payload, Exception e, boolean backed) {
        rabbitErrors.incrementAndGet();
        logger.warning(oneLine(String.format("erroneous payload: "
            + "%s "
            + "exception thrown: "
            + "%s%s",
            payload, e, backed ? "\npayload moved to error queue" : "")));
    }

    /**
     * Indicates that connection to RabbitMQ has been lost.
     *
     * @param rabbit RabbitMQ
     */
    public void rabbitPingError(RabbitMQ rabbit) {
        logger.severe(
            String.format("lost connection to RabbitMQ: %s", oneLine(rabbit)));
    }

    /**
     * Indicates successful reconnection to RabbitMQ.
     *
     * @param rabbit RabbitMQ
     */
    public void rabbitReconnectSuccess(RabbitMQ rabbit) {
        logger.info(String.format(
            "successfully reconnected to RabbitMQ: %s", oneLine(rabbit)));
    }

    /**
     * Indicates error reconnecting to RabbitMQ.
     *
     * @param rabbit RabbitMQ
     * @param e exception
     */
    public void rabbitReconnectError(RabbitMQ rabbit, Exception e) {
        logger.severe(oneLine(String.format("could not reconnect to RabbitMQ:%n"
            + "%s%n"
            + "exception thrown:%n"
            + "%s", rabbit, e)));
    }

    /**
     * Indicates that InitialState has been instantiated.
     *
     * @param is InitialState
     */
    public void initialStateCreated(InitialState is) {
        logger.info(String.format("InitialState created: %s", oneLine(is)));
    }

    /**
     * Indicates that a payload has been successfully written to InitialState.
     */
    public void initialStateWrote() {
        initialStateWrote.incrementAndGet();
    }

    /**
     * Indicates that the given number of payloads have been written to
     * InitialState.
     *
     * @param count number of payloads
     */
    public void initialStateWrote(int count) {
        initialStateWrote.addAndGet(count);
    }

    /**
     * Starts normal logging.
     */
    private void normalLog() {
        if (normalLog != null) {
            Timer timer = new Timer();
            timer.scheduleAtFixedRate(new TimerTask() {
                @Override
                public void run() {
                    long errors = rabbitErrors.getAndSet(0);
                    String msg = oneLine(String.format(
                        "read: %d, backed: %d, written: %d, %s: %d",
                        rabbitRead.getAndSet(0),
                        rabbitBacked.getAndSet(0),
                        initialStateWrote.getAndSet(0),
                        errors == 0 ? "errors" : "ERRORS", errors));
                    logger.fine(msg);
                }
            }, normalLogInterval, normalLogInterval);
        }
    }

    /**
     * One-line string representation of object.
     *
     * @param o object
     * @return string
     */
    private static String oneLine(Object o) {
        return o.toString().replace('\n', ' ');
    }

    /**
     * @return the normal log
     */
    public String getNormalLog() {
        return normalLog;
    }

    /**
     * @return the maximum number of normal log files
     */
    public int getNormalLogNum() {
        return normalLogNum;
    }

    /**
     * @return the maximum size of each normal log file, in bytes
     */
    public int getNormalLogSize() {
        return normalLogSize;
    }

    /**
     * @return the error log
     */
    public String getErrorLog() {
        return errorLog;
    }

    /**
     * @return the maximum number of error log files
     */
    public int getErrorLogNum() {
        return errorLogNum;
    }

    /**
     * @return the maximum size of each error log file, in bytes
     */
    public int getErrorLogSize() {
        return errorLogSize;
    }

    @Override
    public String toString() {
        return String.format("[Log:%n"
            + "normalLog=%s%n"
            + "normalLogNum=%d%n"
            + "normalLogSize=%d%n"
            + "errorLog=%s%n"
            + "errorLogNum=%d%n"
            + "errorLogSize=%d]",
            normalLog, normalLogNum, normalLogSize,
            errorLog, errorLogNum, errorLogSize);
    }
}
