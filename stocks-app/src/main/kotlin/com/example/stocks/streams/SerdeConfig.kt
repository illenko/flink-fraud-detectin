package com.example.stocks.streams

import com.illenko.avro.StockTickerData
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
    fun stockTickerDataSerde(config: Map<String, String>): SpecificAvroSerde<StockTickerData> = createSerde(config)

    private fun <T : SpecificRecord> createSerde(serdeConfig: Map<String, String>): SpecificAvroSerde<T> =
        SpecificAvroSerde<T>().apply {
            println("Configuring serde with $serdeConfig")
            configure(serdeConfig, false)
        }
}
