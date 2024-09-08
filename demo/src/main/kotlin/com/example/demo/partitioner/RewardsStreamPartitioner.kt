package com.example.demo.partitioner

import com.illenko.avro.Purchase
import org.apache.kafka.streams.processor.StreamPartitioner
import org.springframework.stereotype.Component

@Component
class RewardsStreamPartitioner : StreamPartitioner<String, Purchase> {
    override fun partition(
        topic: String,
        key: String,
        value: Purchase,
        numPartitions: Int,
    ): Int = value.customerId.hashCode() % numPartitions
}
