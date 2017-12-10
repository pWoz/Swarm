package pro.woz.swarm.kafkaClients;

import org.apache.kafka.common.PartitionInfo;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class FixedPartitionPickerTest {

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionForEmptyPartitionsList() {
        FixedPartitionPicker partitionPicker = new FixedPartitionPicker(Collections.EMPTY_LIST);
        partitionPicker.pick();
    }

    @Test(expected = RuntimeException.class)
    public void shouldThrowExceptionForNullPartitionsList() {
        FixedPartitionPicker partitionPicker = new FixedPartitionPicker(null);
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
        FixedPartitionPicker partitionPicker = new FixedPartitionPicker(partitions);
        //then
        Assert.assertEquals(0, partitionPicker.pick());
        Assert.assertEquals(0, partitionPicker.pick());
        Assert.assertEquals(0, partitionPicker.pick());
        Assert.assertEquals(0, partitionPicker.pick());
    }

    @Test
    public void shouldReturnAlwaysSamePartitionForMultiplePartitionTopic() {
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
        FixedPartitionPicker partitionPicker = new FixedPartitionPicker(partitions);
        int pickedPartition = partitionPicker.pick();
        //then
        Assert.assertEquals(pickedPartition, partitionPicker.pick());
        Assert.assertEquals(pickedPartition, partitionPicker.pick());
        Assert.assertEquals(pickedPartition, partitionPicker.pick());
        Assert.assertEquals(pickedPartition, partitionPicker.pick());
        Assert.assertEquals(pickedPartition, partitionPicker.pick());
        Assert.assertEquals(pickedPartition, partitionPicker.pick());
    }
}
