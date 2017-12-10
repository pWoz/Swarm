package pro.woz.swarm.kafkaClients;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.*;

public class MessagesConsumer implements Runnable {

    private final KafkaConsumer<String, String> consumer;
    private final List<String> topics;
    private final int id;

    public MessagesConsumer(int id,
                            String groupId,
                            List<String> topics) {
        this.topics = topics;
        this.id = id;
        Properties config = prepareConfig();
        config.put("group.id", groupId);
        consumer = new KafkaConsumer<>(config);
    }

    @Override
    public void run() {
        try {
            consumer.subscribe(topics);
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

    private Properties prepareConfig() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "consumer-tutorial");
        props.put("key.deserializer", StringDeserializer.class.getName());
        props.put("value.deserializer", StringDeserializer.class.getName());
        return props;
    }
}
