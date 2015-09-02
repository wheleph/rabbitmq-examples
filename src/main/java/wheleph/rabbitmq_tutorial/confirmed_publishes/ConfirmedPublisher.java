package wheleph.rabbitmq_tutorial.confirmed_publishes;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Method;
import com.rabbitmq.client.ShutdownListener;
import com.rabbitmq.client.ShutdownSignalException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class ConfirmedPublisher {
    private static final Logger logger = LoggerFactory.getLogger(ConfirmedPublisher.class);

    private final static String EXCHANGE_NAME = "confirmed.publishes";

    private static volatile Channel channel;

    public static void main(String[] args) throws IOException, InterruptedException, TimeoutException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");
        connectionFactory.setPort(5672);

        final Connection connection = connectionFactory.newConnection();

        for (int i = 0; i < 100; i++) {
            if (channel == null) {
                channel = connection.createChannel();
                channel.confirmSelect();
                channel.addShutdownListener(new ShutdownListener() {
                    public void shutdownCompleted(ShutdownSignalException cause) {
                        logger.debug("Handling channel shutdown...", cause);
                        Method reasonMethod = cause.getReason();
                        if (reasonMethod instanceof AMQP.Channel.Close) {
                            AMQP.Channel.Close closeMethod = (AMQP.Channel.Close) reasonMethod;
                            logger.debug("The method is of type Close (replyCode = {})", closeMethod.getReplyCode(), cause);
                            try {
                                if (closeMethod.getReplyCode() != 200) {
                                    // Cannot directly recreate channel here because any blocking call on connection instance here causes deadlock
                                    channel = null;
                                }
                            } catch (Exception e) {
                                logger.error("Failed to create channel", e);
                            }
                        }
                        logger.error("Done handling channel shutdown...", cause);
                    }
                });
            }
            String message = "Hello world" + i;
            channel.basicPublish(EXCHANGE_NAME, "", null, message.getBytes());
            logger.info(" [x] Sent '" + message + "'");
            Thread.sleep(2000);
        }

        channel.close();
        connection.close();
    }
}
