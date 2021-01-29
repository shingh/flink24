package org.apache.flink.streaming.connectors.kafka.partitioner;

import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.Serializable;
import java.util.Map;

/**
 * Hash partition.
 * @param <T>
 */
public class HashPartitioner<T> extends FlinkKafkaPartitioner<T> implements Serializable {
//	private static final long serialVersionUID = 8103823686368682159L;
	ObjectMapper objectMapper;
	String hashPartitionColumn;

	public HashPartitioner(String hashPartitionColumn) {
		this.hashPartitionColumn = hashPartitionColumn;
	}

	public void open(int parallelInstanceId, int parallelInstances) {
		Preconditions.checkArgument((parallelInstanceId >= 0), "Id of this subtask cannot be negative.");
		Preconditions.checkArgument((parallelInstances > 0), "Number of subtasks must be larger than 0.");

		this.objectMapper = new ObjectMapper();
	}

	public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
		Preconditions.checkArgument((partitions != null && partitions.length > 0), "Partitions of the target topic is empty.");

		try {
			Map content = this.objectMapper.readValue(new String(value), Map.class);
			String partitionColumn = String.valueOf(content.get(hashPartitionColumn));
			return partitions[Math.abs(partitionColumn.hashCode()) % partitions.length];
		} catch (Exception e) {
			return partitions[0];
		}
	}

	public boolean equals(Object o) {
		return (this == o || o instanceof HashPartitioner);
	}

	public int hashCode() {
		return HashPartitioner.class.hashCode();
	}
}

