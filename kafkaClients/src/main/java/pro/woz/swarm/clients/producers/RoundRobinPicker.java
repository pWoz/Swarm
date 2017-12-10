package pro.woz.swarm.clients.producers;

import org.apache.kafka.common.PartitionInfo;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.List;

/**
 * Picks the partition from given partitions list that will be used while message production.
 */
public class    RoundRobinPicker implements PartitionPicker {

    public static final Logger LOGGER = LogManager.getLogger(RoundRobinPicker.class.getName());

    private List<PartitionInfo> availablePartitions;
    private int lastPickPosition = -1;

    public RoundRobinPicker(List<PartitionInfo> availablePartitions) {
        if(availablePartitions.isEmpty())
            throw new RuntimeException();
        this.availablePartitions = availablePartitions;
    }

    public int pick() {
        int newPartition = (lastPickPosition + 1) % availablePartitions.size();
        lastPickPosition = newPartition;
        LOGGER.info("Picking partition: " + newPartition);
        return availablePartitions.get(newPartition).partition();
    }
}
