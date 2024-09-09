package com.example.demo.streams

import com.illenko.avro.CorrelatedPurchase
import com.illenko.avro.Purchase
import com.illenko.avro.PurchasePattern
import com.illenko.avro.RewardAccumulator
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.avro.specific.SpecificRecord
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class SerdeConfig {
    @Bean
    fun kafkaSerdeConfig(
        @Value("\${spring.kafka.streams.properties.schema.registry.url}") schemaRegistryUrl: String,
    ): Map<String, String> = mapOf("schema.registry.url" to schemaRegistryUrl)

    @Bean
    fun purchaseSerde(config: Map<String, String>): SpecificAvroSerde<Purchase> = createSerde(config)

    @Bean
    fun purchasePatternSerde(config: Map<String, String>): SpecificAvroSerde<PurchasePattern> = createSerde(config)

    @Bean
    fun rewardAccumulatorSerde(config: Map<String, String>): SpecificAvroSerde<RewardAccumulator> = createSerde(config)

    @Bean
    fun correlatedPurchaseSerde(config: Map<String, String>): SpecificAvroSerde<CorrelatedPurchase> = createSerde(config)

    private fun <T : SpecificRecord> createSerde(serdeConfig: Map<String, String>): SpecificAvroSerde<T> =
        SpecificAvroSerde<T>().apply {
            configure(serdeConfig, false)
        }
}
