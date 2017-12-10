package pro.woz.swarm.kafkaClients;

import org.apache.kafka.common.PartitionInfo;

import java.util.List;
import java.util.Random;

public class FixedPartitionPicker implements PartitionPicker {

    private List<PartitionInfo> availablePartitions;
    private int pickedPartition = -1;

    public FixedPartitionPicker(List<PartitionInfo> availablePartitions) {
        if (availablePartitions.isEmpty())
            throw new RuntimeException();
        this.availablePartitions = availablePartitions;
    }

    @Override
    public int pick() {
        if (pickedPartition == -1) {
            pickedPartition = selectFixedPartition();
        }
        return pickedPartition;
    }

    private int selectFixedPartition() {
        Random r = new Random();
        return r.nextInt(availablePartitions.size());
    }
}
