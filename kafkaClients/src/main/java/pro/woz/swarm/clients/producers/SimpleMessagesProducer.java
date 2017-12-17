package pro.woz.swarm.clients.producers;

import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Sends messages to kafka in round robin manner regarding partitions.
 *
 * @author pwozniak
 */
public class SimpleMessagesProducer {

    public static final Logger LOGGER = LogManager.getLogger(SimpleMessagesProducer.class.getName());

    private Producer producer;
    private String topicName;

    public SimpleMessagesProducer(Producer kafkaProducer, String topicName) {
        this.producer = kafkaProducer;
        this.topicName = topicName;
    }

    public void produceMessage(String key, String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> message = new ProducerRecord<>(topicName, key, value);
        sendMessage(message);
    }

    public void produceMessage(String value) throws ExecutionException, InterruptedException {
        ProducerRecord<String, String> message = new ProducerRecord<>(topicName, value);
        sendMessage(message);
    }

    private void sendMessage(ProducerRecord<String, String> message) throws ExecutionException, InterruptedException {
        Future<RecordMetadata> meta = producer.send(message);
        RecordMetadata recordMetadata = meta.get();
        LOGGER.info("Message sent to broker. Topic " + recordMetadata.topic() + ", partition: " + recordMetadata.partition() + ", offset: " + recordMetadata.offset());
    }
}
