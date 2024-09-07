package com.example.demo.streams

import com.illenko.avro.PurchaseRecord
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.KStream
import org.apache.kafka.streams.kstream.Produced
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class KafkaStreamsConfig {
    @Bean
    fun kStream(
        @Value("\${spring.kafka.streams.properties.schema.registry.url}") schemaRegistryUrl: String,
        builder: StreamsBuilder,
    ): KStream<String, PurchaseRecord> {
        val serdeConfig = mapOf("schema.registry.url" to schemaRegistryUrl)
        val valueSerde = SpecificAvroSerde<PurchaseRecord>()
        valueSerde.configure(serdeConfig, false)

        val stream =
            builder
                .stream("purchase", Consumed.with(Serdes.String(), valueSerde))
                .mapValues { p ->
                    p.apply {
                        this.creditCardNumber = "**** **** **** " + this.creditCardNumber.takeLast(4)
                    }
                }

        stream.to("purchase2", Produced.with(Serdes.String(), valueSerde))
        return stream
    }
}
