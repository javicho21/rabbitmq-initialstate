package main;

import config.Configuration;
import initialstate.InitialState;
import rabbitmq.RabbitMQ;

import java.io.File;
import java.io.IOException;
import java.text.ParseException;
import java.util.concurrent.TimeoutException;

import joptsimple.OptionException;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import joptsimple.OptionSpec;

/**
 * Runs application.
 * 
 * @author aaronzhang
 */
public class Main {
    
    /**
     * Usage error message.
     */
    private static final String USAGE =
        "Usage: ./run [--verify-timestamp] [configFile]\n"
        + "--verify-timestamp: infers timestamp unit; otherwise assumes "
        + "timestamp is in nanoseconds\n"
        + "configFile: specifies configuration file; otherwise checks current "
        + "directory for .rabbitmq-initialstate";
    
    /**
     * Runs application.  If an argument is provided, the argument will be used
     * to find the configuration file.  Otherwise, search the current directory
     * for the configuration file
     * 
     * @param args arguments
     * @throws IOException if error reading configuration file
     * @throws ParseException if error parsing configuration file
     * @throws TimeoutException if error consuming from RabbitMQ
     */
    public static void main(String[] args) throws IOException, ParseException,
        TimeoutException {
        // Parse options
        boolean verifyTimestamp = false;
        File configFile = new File(".rabbitmq-initialstate");
        OptionParser parser = new OptionParser();
        parser.accepts("verify-timestamp");
        OptionSpec<File> configSpec = parser.nonOptions().ofType(File.class);
        try {
            OptionSet options = parser.parse(args);
            if (options.has("verify-timestamp")) {
                verifyTimestamp = true;
            }
            if (options.hasArgument(configSpec)) {
                configFile = options.valueOf(configSpec);
            }
        } catch (OptionException e) {
            System.err.println(USAGE);
            System.exit(1);
        }
        
        // Read configuration file
        Configuration config = new Configuration(configFile);
        config.read();
        // Build log
        Log log = new Log.Builder()
            .setNormalLog(config.get("NORMAL_LOG"))
            .setNormalLogNum(Integer.parseInt(
                config.getOrDefault("NORMAL_LOG_NUM", "0")))
            .setNormalLogSize(Integer.parseInt(
                config.getOrDefault("NORMAL_LOG_SIZE", "0")))
            .setNormalLogInterval(Long.parseLong(
                config.getOrDefault("NORMAL_LOG_INTERVAL", "0")))
            .setErrorLog(config.get("ERROR_LOG"))
            .setErrorLogNum(Integer.parseInt(
                config.getOrDefault("ERROR_LOG_NUM", "0")))
            .setErrorLogSize(Integer.parseInt(
                config.getOrDefault("ERROR_LOG_SIZE", "0")))
            .build();
        // Build RabbitMQ
        RabbitMQ rabbitMQ = new RabbitMQ.Builder()
            .setHost(config.getOrDefault("RABBITMQ_HOST", "localhost"))
            .setPort(Integer.parseInt(
                config.getOrDefault("RABBITMQ_PORT", "5672")))
            .setUsername(config.get("RABBITMQ_USERNAME"))
            .setPassword(config.get("RABBITMQ_PASSWORD"))
            .setVirtualHost(config.getOrDefault("RABBITMQ_VIRTUAL_HOST", "/"))
            .setQueue(config.get("RABBITMQ_QUEUE"))
            .setBackupQueue(config.get("RABBITMQ_BACKUP_QUEUE"))
            .setErrorQueue(config.get("RABBITMQ_ERROR_QUEUE"))
            .setVerifyTimestamp(verifyTimestamp)
            .setLog(log)
            .build();
        // Build InitialState
        InitialState initialState = new InitialState.Builder()
            .setKey(config.get("INITIALSTATE_KEY"))
            .setPointsToFlush(Integer.parseInt(config.getOrDefault(
                "INITIALSTATE_POINTS_FLUSH", "1000")))
            .setMillisToFlush(Integer.parseInt(config.getOrDefault(
                "INITIALSTATE_MILLIS_FLUSH", "5000")))
            .setLog(log)
            .build();
        
        // Setup communication between RabbitMQ and InitialState
        rabbitMQ.addObserver(initialState);
        // Consume messages from RabbitMQ
        rabbitMQ.consume();
        // Periodically ping RabbitMQ
        rabbitMQ.ping();
    }
}