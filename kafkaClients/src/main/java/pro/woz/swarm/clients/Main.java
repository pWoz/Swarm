package pro.woz.swarm.clients;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.common.serialization.StringSerializer;
import pro.woz.swarm.clients.producers.SimpleMessagesProducer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        Properties config = prepareConfig();
        //
        SimpleMessagesProducer producer = new SimpleMessagesProducer(new KafkaProducer<>(config),"topic");
        for (int i = 0; i < 100; i++) {
            producer.produceMessage("ala ma kota " + i);
        }
    }

    private static Properties prepareConfig() {
        Properties config = new Properties();
        config.put("client.id", "producer");
        config.put("group.id", "producers");
        config.put("bootstrap.servers", "127.0.0.1:9092");
        config.put("key.serializer", StringSerializer.class.getName());
        config.put("value.serializer", StringSerializer.class.getName());
        return config;
    }
}
