package pro.woz.swarm.kafkaClients;

import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class RoundRobinPickerTest {

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionForEmptyPartitionsList() {
        RoundRobinPicker partitionPicker = new RoundRobinPicker(Collections.EMPTY_LIST);
        partitionPicker.pick();
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionForNullPartitionsList() {
        RoundRobinPicker partitionPicker = new RoundRobinPicker(null);
        partitionPicker.pick();
    }

    @Test
    public void shouldReturnAlwaysSamePartitionForSinglePartitionTopic() {
        //given
        PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
        Mockito.when(partitionInfo.partition()).thenReturn(0);
        List<PartitionInfo> partitions = new ArrayList();
        partitions.add(partitionInfo);
        //when
        RoundRobinPicker partitionPicker = new RoundRobinPicker(partitions);
        //then
        Assert.assertEquals(0, partitionPicker.pick());
        Assert.assertEquals(0, partitionPicker.pick());
        Assert.assertEquals(0, partitionPicker.pick());
        Assert.assertEquals(0, partitionPicker.pick());
    }

    @Test
    public void shouldReturnProperPartition() {
        //given
        PartitionInfo partitionInfo = Mockito.mock(PartitionInfo.class);
        Mockito.when(partitionInfo.partition()).thenReturn(0);
        PartitionInfo partitionInfo1 = Mockito.mock(PartitionInfo.class);
        Mockito.when(partitionInfo1.partition()).thenReturn(1);
        PartitionInfo partitionInfo2 = Mockito.mock(PartitionInfo.class);
        Mockito.when(partitionInfo2.partition()).thenReturn(2);
        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>();
        partitions.add(partitionInfo);
        partitions.add(partitionInfo1);
        partitions.add(partitionInfo2);
        //when
        RoundRobinPicker partitionPicker = new RoundRobinPicker(partitions);
        //then
        Assert.assertEquals(0,partitionPicker.pick());
        Assert.assertEquals(1,partitionPicker.pick());
        Assert.assertEquals(2,partitionPicker.pick());
        Assert.assertEquals(0,partitionPicker.pick());
        Assert.assertEquals(1,partitionPicker.pick());
        Assert.assertEquals(2,partitionPicker.pick());
    }
}
