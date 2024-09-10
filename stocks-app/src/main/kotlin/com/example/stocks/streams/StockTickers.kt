package com.example.stocks.streams

import com.illenko.avro.StockTickerData
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde
import org.apache.kafka.common.serialization.Serdes
import org.apache.kafka.streams.StreamsBuilder
import org.apache.kafka.streams.Topology
import org.apache.kafka.streams.kstream.Consumed
import org.apache.kafka.streams.kstream.Grouped
import org.apache.kafka.streams.kstream.Materialized
import org.apache.kafka.streams.kstream.Printed
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration

@Configuration
class StockTickers(
    private val stockTickerDataSerde: SpecificAvroSerde<StockTickerData>,
) {
    @Bean
    fun topology(builder: StreamsBuilder): Topology {
        val stockTickerStream =
            builder.stream(
                "stock-tickers",
                Consumed.with(Serdes.String(), stockTickerDataSerde),
            )

        val stockTickerTable =
            stockTickerStream
                .groupByKey(Grouped.with(Serdes.String(), stockTickerDataSerde))
                .reduce(
                    { _, newValue -> newValue },
                    Materialized.with(Serdes.String(), stockTickerDataSerde),
                )

        stockTickerTable.toStream().print(Printed.toSysOut<String, StockTickerData>().withLabel("stock-ticker"))

        return builder.build()
    }
}
