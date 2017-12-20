package pro.woz.swarm.clients.consumers;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

public class MessagesConsumerTest {

    public MockConsumer<String, String> consumer;

    @Before
    public void setUp() throws Exception {
        consumer = new MockConsumer<String, String>(OffsetResetStrategy.EARLIEST);
    }

    @Test
    public void shouldConsume() throws InterruptedException {
        consumer.assign(Arrays.asList(new TopicPartition("topic", 0)));

        HashMap<TopicPartition, Long> beginningOffsets = new HashMap<>();
        beginningOffsets.put(new TopicPartition("topic", 0), 0L);
        consumer.updateBeginningOffsets(beginningOffsets);

        consumer.addRecord(new ConsumerRecord<String, String>("topic",
                0, 0L, "mykey", "myvalue0"));
        consumer.addRecord(new ConsumerRecord<String, String>("topic", 0,
                1L, "mykey", "myvalue1"));
        consumer.addRecord(new ConsumerRecord<String, String>("topic", 0,
                2L, "mykey", "myvalue2"));
        consumer.addRecord(new ConsumerRecord<String, String>("topic", 0,
                3L, "mykey", "myvalue3"));
        consumer.addRecord(new ConsumerRecord<String, String>("topic", 0,
                4L, "mykey", "myvalue4"));


        MessagesConsumer con = new MessagesConsumer(consumer,1);
        new Thread(con).start();
        Thread.sleep(1000);
        con.shutdown();
    }
}