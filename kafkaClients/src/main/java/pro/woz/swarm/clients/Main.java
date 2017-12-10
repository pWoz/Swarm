package pro.woz.swarm.clients;

import pro.woz.swarm.clients.producers.MessagesProducer;

import java.util.concurrent.ExecutionException;

public class Main {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        MessagesProducer producer = new MessagesProducer("topic");
        for (int i = 0; i < 100; i++) {
            producer.produceMessage("ala ma kota " + i);
        }
    }
}
