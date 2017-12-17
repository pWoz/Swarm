package pro.woz.swarm.clients;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import pro.woz.swarm.clients.consumers.MessagesConsumer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class Consumers {

    public static final Logger LOGGER = LogManager.getLogger(Consumers.class.getName());

    public static void main(String[] args) {
        int numConsumers = 3;
        String groupId = "consumer-tutorial-group";
        List<String> topics = Arrays.asList("topic");
        ExecutorService executor = Executors.newFixedThreadPool(numConsumers);

        final List<MessagesConsumer> consumers = new ArrayList<>();
        for (int i = 0; i < numConsumers; i++) {
            MessagesConsumer consumer = new MessagesConsumer(i, groupId, topics);
            consumers.add(consumer);
            executor.submit(consumer);
        }

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                for (MessagesConsumer consumer : consumers) {
                    consumer.shutdown();
                }
                executor.shutdown();
                try {
                    executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
                } catch (InterruptedException e) {
                    LOGGER.error(e);
                    Thread.currentThread().interrupt();
                }
            }
        });
    }
}
