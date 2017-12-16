package pro.woz.swarm.kafkaClients;

import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import pro.woz.swarm.clients.producers.SimpleMessagesProducer;

import java.util.List;
import java.util.concurrent.ExecutionException;

public class SimpleMessagesProducerTest {

    private MockProducer<String, String> producer;

    private static final String MESSAGE_KEY = "sampleKey";
    private static final String MESSAGE = "sampleMessage";

    @Before
    public void setUp() {
        producer = new MockProducer<>(true, new StringSerializer(), new StringSerializer());
    }

    @Test
    public void keyLessMessageShouldBeSentCorrectly() throws ExecutionException, InterruptedException {
        //given
        SimpleMessagesProducer pr = new SimpleMessagesProducer(producer, "sampleTopic");
        //when
        pr.produceMessage(MESSAGE);
        //then
        List<ProducerRecord<String, String>> history = producer.history();
        Assert.assertEquals(1, history.size());
        Assert.assertEquals(MESSAGE, history.get(0).value());
        Assert.assertNull(history.get(0).key());
    }

    @Test
    public void keyedMessageShouldBeSentCorrectly() throws ExecutionException, InterruptedException {
        //given
        SimpleMessagesProducer pr = new SimpleMessagesProducer(producer, "sampleTopic");
        //when
        pr.produceMessage(MESSAGE_KEY, MESSAGE);
        //then
        List<ProducerRecord<String, String>> history = producer.history();
        Assert.assertEquals(1, history.size());
        Assert.assertEquals(MESSAGE, history.get(0).value());
        Assert.assertNotNull(history.get(0).key());
        Assert.assertEquals(MESSAGE_KEY, history.get(0).key());
    }
}
