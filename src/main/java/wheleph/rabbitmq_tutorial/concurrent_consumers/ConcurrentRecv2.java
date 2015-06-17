package wheleph.rabbitmq_tutorial.concurrent_consumers;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Consumer;
import com.rabbitmq.client.DefaultConsumer;
import com.rabbitmq.client.Envelope;

import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class ConcurrentRecv2 {
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

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        final boolean autoAck = false;

        registerConsumer(channel, "0", autoAck, 500, threadPool);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Invoking shutdown hook...");
                System.out.println("Shutting down thread pool...");
                threadPool.shutdown();
                try {
                    while(!threadPool.awaitTermination(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    System.out.println("Interrupted while waiting for termination");
                }
                System.out.println("Thread pool shut down.");
                System.out.println("Closing connection...");
                try {
                    connection.close();
                } catch (IOException e) {
                    System.out.println("Exception while closing a connection");
                }
                System.out.println("Connection closed.");
                System.out.println("Done with shutdown hook.");
            }
        });
    }

    private static void registerConsumer(final Channel channel, final String channelName, final boolean autoAck, final int timeout, final ExecutorService threadPool) throws IOException {
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queueBind(QUEUE_NAME, QUEUE_NAME, "");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, final Envelope envelope, AMQP.BasicProperties properties, final byte[] body) throws IOException {
                try {
                    System.out.printf("Received %s/%s %s%n", channelName, Thread.currentThread().getName(), new String(body));

                    threadPool.submit(new Runnable() {
                        public void run() {
                            try {
                                System.out.printf("Processing %s %s%n", Thread.currentThread().getName(), new String(body));
                                Thread.sleep(timeout);
                                System.out.printf("Processed %s %s%n", Thread.currentThread().getName(), new String(body));

                                System.out.println(String.format("Sending ack for %s: %b", new String(body), autoAck));
                                if (!autoAck) {
                                    try {
                                        getChannel().basicAck(envelope.getDeliveryTag(), false);
                                        System.out.println(String.format("Sent ack for %s", new String(body)));
                                    } catch (Exception e) {
                                        e.printStackTrace();
                                    }
                                }
                            } catch (InterruptedException e) {
                                System.out.printf("Interrupted %s %s%n", Thread.currentThread().getName(), new String(body));
                            }
                        }
                    });
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        };

        channel.basicConsume(QUEUE_NAME, autoAck, consumer);
    }
}
