package wheleph.rabbitmq_tutorial.concurrent_consumers;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class MultipleSend {
    private static final Logger logger = LoggerFactory.getLogger(MultipleSend.class);

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) throws IOException {
        ConnectionFactory connectionFactory = new ConnectionFactory();
        connectionFactory.setHost("localhost");

        Connection connection = connectionFactory.newConnection();
        Channel channel = connection.createChannel();

        channel.exchangeDeclare(QUEUE_NAME, "fanout");
        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queueBind(QUEUE_NAME, QUEUE_NAME, "");

        for (int i = 0; i < 100; i++) {
            String message = "Hello world" + i;
            channel.basicPublish("", QUEUE_NAME, null, message.getBytes());
            logger.info(" [x] Sent '" + message + "'");
        }

        channel.close();
        connection.close();
    }
}
