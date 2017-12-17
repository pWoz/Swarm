package pro.woz.swarm.clients;

import org.apache.kafka.clients.consumer.ConsumerRecords;

public interface EventConsumer<String, V> {

    public void consume(ConsumerRecords<String, V> record);
}
