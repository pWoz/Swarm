package pro.woz.swarm.clients.consumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import pro.woz.swarm.clients.EventConsumer;

import java.util.*;

public class MessagesConsumer implements Runnable {

    private final Consumer<String, String> consumer;
    private List<EventConsumer> eventConsumers;
    private final int id;

    public MessagesConsumer(Consumer<String, String> kafkaConsumer, int id) {
        consumer = kafkaConsumer;
        this.id = id;
        eventConsumers = Collections.emptyList();
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
                notifyCosumers(records);
                for (ConsumerRecord<String, String> record : records) {
                    Map<String, Object> data = new HashMap<>();
                    data.put("partition", record.partition());
                    data.put("offset", record.offset());
                    data.put("value", record.value());
                    System.out.println(this.id + ": " + data);
                }
            }
        } catch (WakeupException e) {
            //ignore
        } finally {
            consumer.close();
        }
    }

    public void shutdown() {
        consumer.wakeup();
    }

    private void notifyCosumers(ConsumerRecords<String, String> records) {
        for (EventConsumer eventConsumer : eventConsumers) {
            eventConsumer.consume(records);
        }
    }

    public void subscribeConsumer(EventConsumer consumer) {
        eventConsumers.add(consumer);
    }
}
