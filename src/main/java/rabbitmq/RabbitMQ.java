package rabbitmq;

import main.Log;

import java.io.IOException;
import java.util.Observable;
import java.util.concurrent.TimeoutException;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import java.util.Timer;
import java.util.TimerTask;

/**
 * Consumes messages from RabbitMQ. This class is observable and observers will
 * be notified when new payloads are consumed.
 *
 * @author aaronzhang
 */
public class RabbitMQ extends Observable {

    /**
     * Host.
     */
    private String host;

    /**
     * Port.
     */
    private int port;

    /**
     * Username.
     */
    private String username;

    /**
     * Password.
     */
    private String password;

    /**
     * Virtual host.
     */
    private String virtualHost;

    /**
     * Queue to consume.
     */
    private String queue;

    /**
     * Name of backup queue, or {@code null} if queue should not be backed up.
     */
    private String backupQueue;

    /**
     * Name of error queue, or {@code null} if no error queue.
     */
    private String errorQueue;

    /**
     * Whether to verify timestamp unit. If not, assumes nanoseconds.
     */
    private boolean verifyTimestamp = false;

    /**
     * Log.
     */
    private Log log;

    /**
     * RabbitMQ connection.
     */
    private Connection connection;

    /**
     * RabbitMQ builder.
     */
    public static class Builder {

        /**
         * RabbitMQ being constructed.
         */
        private final RabbitMQ internal;

        /**
         * New builder.
         */
        public Builder() {
            internal = new RabbitMQ();
        }

        /**
         * Sets host.
         *
         * @param host host
         * @return this builder
         */
        public Builder setHost(String host) {
            internal.host = host;
            return this;
        }

        /**
         * Sets port.
         *
         * @param port port
         * @return this builder
         */
        public Builder setPort(int port) {
            internal.port = port;
            return this;
        }

        /**
         * Sets username.
         *
         * @param username username
         * @return this builder
         */
        public Builder setUsername(String username) {
            internal.username = username;
            return this;
        }

        /**
         * Sets password.
         *
         * @param password password
         * @return this builder
         */
        public Builder setPassword(String password) {
            internal.password = password;
            return this;
        }

        /**
         * Sets virtual host.
         *
         * @param virtualHost virtual host
         * @return this builder
         */
        public Builder setVirtualHost(String virtualHost) {
            internal.virtualHost = virtualHost;
            return this;
        }

        /**
         * Sets queue to consume.
         *
         * @param queue queue
         * @return this builder
         */
        public Builder setQueue(String queue) {
            internal.queue = queue;
            return this;
        }

        /**
         * Sets backup queue.
         *
         * @param backupQueue backup queue
         * @return this builder
         */
        public Builder setBackupQueue(String backupQueue) {
            internal.backupQueue = backupQueue;
            return this;
        }

        /**
         * Sets error queue.
         *
         * @param errorQueue error queue
         * @return this builder
         */
        public Builder setErrorQueue(String errorQueue) {
            internal.errorQueue = errorQueue;
            return this;
        }

        /**
         * Whether to verify the timestamp unit. Defaults to false.
         *
         * @param verifyTimestamp whether to verify timestamp
         * @return this builder
         */
        public Builder setVerifyTimestamp(boolean verifyTimestamp) {
            internal.verifyTimestamp = verifyTimestamp;
            return this;
        }

        /**
         * Log. If not set, this object will not log messages.
         *
         * @param log log
         * @return this builder
         */
        public Builder setLog(Log log) {
            internal.log = log;
            return this;
        }

        /**
         * Builds RabbitMQ.
         *
         * @return RabbitMQ
         */
        public RabbitMQ build() {
            internal.parser
                = new LineMultipleValuesParser(internal.verifyTimestamp);
            if (internal.log != null) {
                internal.log.rabbitCreated(internal);
            }
            return internal;
        }
    }

    /**
     * Parses RabbitMQ payload strings.
     */
    private RabbitMQParser parser;

    /**
     * Instantiates RabbitMQ.
     */
    private RabbitMQ() {

    }

    /**
     * Starts consuming data from RabbitMQ.
     *
     * @throws IOException error connecting to RabbitMQ
     * @throws TimeoutException error connecting to RabbitMQ
     */
    public void consume() throws IOException, TimeoutException {
        // Open a connection with the given parameters
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(host);
        factory.setPort(port);
        factory.setUsername(username);
        factory.setPassword(password);
        factory.setVirtualHost(virtualHost);
        // Detect if connection is lost
        factory.setRequestedHeartbeat(5);
        try {
            connection = factory.newConnection();
        } catch (IOException e) {
            throw new IOException("Couldn't connect to RabbitMQ");
        } catch (TimeoutException e) {
            throw new TimeoutException("Couldn't connect to RabbitMQ");
        }

        // Channel and appropriate queues
        Channel channel = connection.createChannel();
        channel.queueDeclarePassive(queue);
        if (backupQueue != null) {
            channel.queueDeclarePassive(backupQueue);
        }
        if (errorQueue != null) {
            channel.queueDeclarePassive(errorQueue);
        }

        // Consumer
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope,
                AMQP.BasicProperties properties, byte[] body)
                throws IOException {
                if (log != null) {
                    log.rabbitRead();
                }
                // Publish to backup queue, if any
                if (backupQueue != null) {
                    channel.basicPublish("", backupQueue, null, body);
                    if (log != null) {
                        log.rabbitBacked();
                    }
                }
                // Notify observers
                setChanged();
                String payload = new String(body);
                try {
                    notifyObservers(parser.parse(payload));
                } catch (Exception e) {
                    // Publish erronous payloads to error queue, if any
                    boolean backed = false;
                    if (errorQueue != null) {
                        channel.basicPublish("", errorQueue, null, body);
                        backed = true;
                    }
                    log.rabbitError(payload, e, backed);
                }
            }
        };
        channel.basicConsume(queue, true, consumer);
    }

    /**
     * Periodically checks if the connection is open.
     */
    public void ping() {
        Timer timer = new Timer();
        timer.scheduleAtFixedRate(new TimerTask() {
            @Override
            public void run() {
                if (!connection.isOpen()) {
                    timer.cancel();
                    if (log != null) {
                        log.rabbitPingError(RabbitMQ.this);
                    }
                    // Attempt to reconnect
                    reconnect();
                    ping();
                }
            }
        }, 5000, 5000);
    }

    /**
     * Attempts to reconnect to RabbitMQ.
     */
    private void reconnect() {
        for (int i = 0; i < 4; i++) {
            try {
                Thread.sleep(15000);
            } catch (InterruptedException e) {

            }
            try {
                // Try to reconnect
                consume();
                log.rabbitReconnectSuccess(this);
                return;
            } catch (Exception e) {
                log.rabbitReconnectError(this, e);
            }
        }
        // Stop trying after 4 failed attempts
        System.exit(1);
    }

    /**
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * @return the username
     */
    public String getUsername() {
        return username;
    }

    /**
     * @return the password
     */
    public String getPassword() {
        return password;
    }

    /**
     * @return the virtual host
     */
    public String getVirtualHost() {
        return virtualHost;
    }

    /**
     * @return the queue
     */
    public String getQueue() {
        return queue;
    }

    /**
     * @return the backup queue
     */
    public String getBackupQueue() {
        return backupQueue;
    }

    @Override
    public String toString() {
        return String.format("[RabbitMQ:%n"
            + "host=%s%n"
            + "port=%d%n"
            + "virtualHost=%s%n"
            + "queue=%s%n"
            + "backupQueue=%s]", host, port, virtualHost, queue, backupQueue);
    }
}
