package wheleph.rabbitmq_tutorial.work_queue;

import java.io.IOException;

public class NewTaskDriver {
    public static void main(String[] args) throws IOException {
        NewTask.main(new String[] {"First message."});
        NewTask.main(new String[] {"Second message.."});
        NewTask.main(new String[] {"Third message..."});
        NewTask.main(new String[] {"Fourth message...."});
        NewTask.main(new String[] {"Fifth message....."});
    }
}
