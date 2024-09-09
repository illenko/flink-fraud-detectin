package com.example.demo.extractor

import com.illenko.avro.Purchase
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.streams.processor.TimestampExtractor
import org.springframework.stereotype.Component

@Component
class PurchaseTimestampExtractor : TimestampExtractor {
    override fun extract(
        record: ConsumerRecord<Any, Any>,
        partitionTime: Long,
    ): Long = (record.value() as Purchase).purchaseDate.toEpochMilli()
}
