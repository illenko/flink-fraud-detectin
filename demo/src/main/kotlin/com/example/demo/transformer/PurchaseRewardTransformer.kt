package com.example.demo.transformer

import com.illenko.avro.Purchase
import com.illenko.avro.RewardAccumulator
import org.apache.kafka.streams.kstream.ValueTransformer
import org.apache.kafka.streams.processor.ProcessorContext
import org.apache.kafka.streams.state.KeyValueStore

class PurchaseRewardTransformer(
    private val stateStoreName: String,
) : ValueTransformer<Purchase, RewardAccumulator> {
    private lateinit var stateStore: KeyValueStore<String, Int>
    private lateinit var context: ProcessorContext

    override fun init(context: ProcessorContext) {
        this.context = context
        stateStore = context.getStateStore(stateStoreName) as KeyValueStore<String, Int>
    }

    override fun transform(value: Purchase): RewardAccumulator {
        println("Transforming purchase: $value")

        val rewardAccumulator: RewardAccumulator = value.toReward()
        val accumulatedSoFar = stateStore[rewardAccumulator.customerId] ?: 0
        println("Accumulated so far for customer ${rewardAccumulator.customerId}: $accumulatedSoFar")
        rewardAccumulator.totalRewardPoints += accumulatedSoFar
        stateStore.put(rewardAccumulator.customerId, rewardAccumulator.totalRewardPoints)
        println("Updated reward accumulator: $rewardAccumulator")

        return rewardAccumulator
    }

    override fun close() {
        // no-op
    }

    private fun Purchase.toReward(): RewardAccumulator {
        val rewardPoints = (this.price * this.quantity).toInt()

        return RewardAccumulator
            .newBuilder()
            .setCustomerId("${this.firstName},${this.lastName}")
            .setPurchaseTotal(this.price * this.quantity)
            .setTotalRewardPoints(rewardPoints)
            .setCurrentRewardPoints(rewardPoints)
            .setDaysFromLastPurchase(0)
            .build()
    }
}
