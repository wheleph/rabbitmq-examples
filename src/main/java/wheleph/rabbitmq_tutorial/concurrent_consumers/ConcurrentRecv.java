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

        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queueBind(QUEUE_NAME, QUEUE_NAME, "");

        System.out.println(" [*] Waiting for messages. To exit press CTRL+C");

        final boolean autoAck = false;

        Consumer consumer = new DefaultConsumer(channel) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.printf("Received 0/%s %s%n", Thread.currentThread().getName(), new String(body));

                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                }

                if (!autoAck) {
                    channel.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };

        Consumer consumer1 = new DefaultConsumer(channel1) {
            @Override
            public void handleDelivery(String consumerTag, Envelope envelope, AMQP.BasicProperties properties, byte[] body) throws IOException {
                System.out.printf("Received 1/%s %s%n", Thread.currentThread().getName(), new String(body));
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                }

                if (!autoAck) {
                    channel1.basicAck(envelope.getDeliveryTag(), false);
                }
            }
        };

        channel.basicConsume(QUEUE_NAME, autoAck, consumer);
        channel1.basicConsume(QUEUE_NAME, autoAck, consumer1);

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
}
