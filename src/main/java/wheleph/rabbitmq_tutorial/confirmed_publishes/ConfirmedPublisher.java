package wheleph.rabbitmq_tutorial.confirmed_publishes;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import net.jodah.lyra.ConnectionOptions;
import net.jodah.lyra.Connections;
import net.jodah.lyra.config.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConfirmedPublisher {
    private static final Logger logger = LoggerFactory.getLogger(ConfirmedPublisher.class);
    private final static String EXCHANGE_NAME = "confirmed.publishes";

    // Beware that proper synchronization of channel is needed because current approach may lead to race conditions
    private volatile static Channel channel;

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        Config config = new Config();
        ConnectionOptions connectionOptions = new ConnectionOptions()
                .withHost("localhost")
                .withPort(5672);

        Connection connection = Connections.create(connectionOptions, config);

        for (int i = 0; i < 100; i++) {
            if (channel == null) {
                createChannel(connection);
            }
            String message = "Hello world" + i;
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
            logger.info(" [x] Sent '" + message + "'");
            Thread.sleep(2000);
        }

        channel.close();
        connection.close();
    }

    private static void createChannel(final Connection connection) throws IOException {
        channel = connection.createChannel();
        channel.confirmSelect(); // This in fact is not necessary
//        channel.addShutdownListener(new ShutdownListener() {
//            public void shutdownCompleted(ShutdownSignalException cause) {
//                // Beware that proper synchronization is needed here
//                logger.debug("Handling channel shutdown...", cause);
//                if (cause.isInitiatedByApplication()) {
//                    logger.debug("Shutdown is initiated by application. Ignoring it.");
//                } else {
//                    logger.error("Shutdown is NOT initiated by application. Resetting the channel.");
//                    /* We cannot re-initialize channel here directly because ShutdownListener callbacks run in the connection's thread,
//                       so the call to createChannel causes a deadlock since it blocks waiting for a response (whilst the connection's thread
//                       is stuck executing the listener). */
//                    channel = null;
//                }
//            }
//        });
    }
}
