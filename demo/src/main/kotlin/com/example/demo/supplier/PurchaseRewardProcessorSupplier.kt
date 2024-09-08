package com.example.demo.supplier

import com.example.demo.transformer.PurchaseRewardTransformer
import com.illenko.avro.Purchase
import com.illenko.avro.RewardAccumulator
import org.apache.kafka.streams.processor.api.FixedKeyProcessor
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext
import org.apache.kafka.streams.processor.api.FixedKeyProcessorSupplier
import org.apache.kafka.streams.processor.api.FixedKeyRecord

class PurchaseRewardProcessorSupplier(
    private val purchaseRewardTransformer: PurchaseRewardTransformer,
) : FixedKeyProcessorSupplier<String, Purchase, RewardAccumulator> {
    override fun get(): FixedKeyProcessor<String, Purchase, RewardAccumulator> =
        object : FixedKeyProcessor<String, Purchase, RewardAccumulator> {
            private lateinit var context: FixedKeyProcessorContext<String, RewardAccumulator>

            override fun init(context: FixedKeyProcessorContext<String, RewardAccumulator>) {
                this.context = context
                purchaseRewardTransformer.init(context.getStateStore("rewardsPointsStore"))
            }

            override fun process(record: FixedKeyRecord<String, Purchase>) {
                val rewardAccumulator = purchaseRewardTransformer.transform(record.value())
                context.forward(record.withValue(rewardAccumulator))
            }

            override fun close() {
                purchaseRewardTransformer.close()
            }
        }
}
