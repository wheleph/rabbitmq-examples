package wheleph.rabbitmq_tutorial.confirmed_publishes;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConfirmListener;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConfirmedPublisher {
    private static final Logger logger = LoggerFactory.getLogger(ConfirmedPublisher.class);

    private final static String EXCHANGE_NAME = "confirmed.publishes";

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);

        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.confirmSelect();
        channel.addConfirmListener(new ConfirmListener() {
            public void handleAck(long deliveryTag, boolean multiple) throws IOException {
                logger.debug(String.format("Received ack for %d (multiple %b)", deliveryTag, multiple));
            }

            public void handleNack(long deliveryTag, boolean multiple) throws IOException {
                logger.debug(String.format("Received nack for %d (multiple %b)", deliveryTag, multiple));
            }
        });

        channel.addShutdownListener(new ShutdownListener() {
            public void shutdownCompleted(ShutdownSignalException cause) {
                logger.error("Channel closed due to error", cause);
            }
        });

        for (int i = 0; i < 100; i++) {
            String message = "Hello world" + channel.getNextPublishSeqNo();
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
            logger.info(" [x] Sent '" + message + "'");
            Thread.sleep(2000);
        }

        channel.close();
        connection.close();
    }
}
