package pro.woz.swarm.clients.consumers;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;

import java.util.*;

public class MessagesConsumer implements Runnable {

    private final Consumer<String, String> consumer;
    private final List<String> topics;
    private final int id;

    public MessagesConsumer(Consumer<String, String> kafkaConsumer, int id,
                            List<String> topics) {
        consumer = kafkaConsumer;
        this.topics = topics;
        this.id = id;
    }

    @Override
    public void run() {
        try {
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Long.MAX_VALUE);
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

}
