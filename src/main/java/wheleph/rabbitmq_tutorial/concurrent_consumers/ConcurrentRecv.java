package wheleph.rabbitmq_tutorial.concurrent_consumers;

import com.rabbitmq.client.*;

import java.io.IOException;

public class ConcurrentRecv {
    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException, InterruptedException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        final Connection connection = connectionFactory.newConnection();
        final Channel channel = connection.createChannel();
        final Channel channel1 = connection.createChannel();

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        final boolean autoAck = false;

        registerConsumer(channel, autoAck, 500);
        registerConsumer(channel1, autoAck, 500);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                System.out.println("Invoking shutdown hook...");
                try {
                    connection.close();
                } catch (IOException e) {
                }
                System.out.println("Done with shutdown hook.");
            }
        });
    }

    private static void registerConsumer(final Channel channel, final boolean autoAck, final int timeout) throws IOException {
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queueBind(QUEUE_NAME, QUEUE_NAME, "");

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.printf("Received 0/%s %s%n", Thread.currentThread().getName(), new String(body));

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
