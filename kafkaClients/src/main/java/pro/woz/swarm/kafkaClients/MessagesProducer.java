package pro.woz.swarm.kafkaClients;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class MessagesProducer {

    private KafkaProducer<String, String> producer;
    private PartitionPicker partitionPicker;
    private String topicName;

    public MessagesProducer(String topic) {
        topicName = topic;
        Properties config = prepareConfig();
        producer = new KafkaProducer<String, String>(config);
        partitionPicker = new RoundRobinPicker(producer.partitionsFor(topicName));
    }

    private Properties prepareConfig() {
        Properties config = new Properties();
        config.put("client.id", "producer");
        config.put("group.id", "producers");
        config.put("bootstrap.servers", "127.0.0.1:9092");
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        return config;
    }

    public void produceMessage(String message) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> m = new ProducerRecord<String, String>(topicName, partitionPicker.pick(), "key", message);
        Future<RecordMetadata> meta = producer.send(m);
        System.out.println("Topic: " + meta.get().topic());
        System.out.println("Partition: " + meta.get().partition());
        System.out.println("Offset: " + meta.get().offset());
    }
}
