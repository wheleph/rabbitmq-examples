package wheleph.rabbitmq_tutorial.concurrent_consumers;

import com.rabbitmq.client.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ConcurrentRecv {
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentRecv.class);

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, InterruptedException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        final Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();
        final Channel channel1 = connection.createChannel();

        logger.info(" [*] Waiting for messages. To exit press CTRL+C");

        final boolean autoAck = false;

        registerConsumer(channel, autoAck, 500);
        registerConsumer(channel1, autoAck, 500);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Invoking shutdown hook...");
                try {
                    connection.close();
                } catch (IOException e) {
                }
                logger.info("Done with shutdown hook.");
            }
        });
    }

    private static void registerConsumer(final Channel channel, final boolean autoAck, final int timeout) throws IOException {
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                logger.info(String.format("Received (channel %d) %s", channel.getChannelNumber(), new String(body)));

                try {
                    Thread.sleep(timeout);
                } catch (InterruptedException e) {
                }

                if (!autoAck) {
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };

        channel.basicConsume(QUEUE_NAME, autoAck, consumer);
    }
}
