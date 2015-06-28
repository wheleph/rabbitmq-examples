package wheleph.rabbitmq_tutorial.concurrent_consumers;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConcurrentRecv2 {
    private static final Logger logger = LoggerFactory.getLogger(ConcurrentRecv2.class);

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, InterruptedException {
        int threadNumber = 2;
        int queueSize = 100;
        final ExecutorService threadPool =  new ThreadPoolExecutor(threadNumber, threadNumber,
                0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<Runnable>(queueSize));

        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        final Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();

        logger.info(" [*] Waiting for messages. To exit press CTRL+C");

        final boolean autoAck = false;

        registerConsumer(channel, autoAck, 500, threadPool);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                logger.info("Invoking shutdown hook...");
                logger.info("Shutting down thread pool...");
                threadPool.shutdown();
                try {
                    while(!threadPool.awaitTermination(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    logger.info("Interrupted while waiting for termination");
                }
                logger.info("Thread pool shut down.");
                logger.info("Closing connection...");
                try {
                    connection.close();
                } catch (IOException e) {
                    logger.info("Exception while closing a connection");
                }
                logger.info("Connection closed.");
                logger.info("Done with shutdown hook.");
            }
        });
    }

    private static void registerConsumer(final Channel channel, final boolean autoAck, final int timeout, final ExecutorService threadPool) throws IOException {
        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, final Envelope envelope, AMQP.BasicProperties properties, final byte[] body) throws IOException {
                try {
                    logger.info(String.format("Received (channel %d) %s", channel.getChannelNumber(), new String(body)));

                    threadPool.submit(new Runnable() {
                        public void run() {
                            try {
                                logger.info(String.format("Processing %s", new String(body)));
                                Thread.sleep(timeout);
                                logger.info(String.format("Processed %s", new String(body)));

                                logger.info(String.format("Sending ack for %s: %b", new String(body), autoAck));
                                if (!autoAck) {
                                    try {
                                        getChannel().basicAck(envelope.getDeliveryTag(), false);
                                        logger.info(String.format("Sent ack for %s", new String(body)));
                                    } catch (Exception e) {
                                        logger.error("", e);
                                    }
                                }
                            } catch (InterruptedException e) {
                                logger.warn(String.format("Interrupted %s", new String(body)));
                            }
                        }
                    });
                } catch (Exception e) {
                    logger.error("", e);
                }
            }
        };

        channel.basicConsume(QUEUE_NAME, autoAck, consumer);
    }
}
