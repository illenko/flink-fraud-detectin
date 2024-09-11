package com.example.stocks.controller

import com.illenko.avro.StockTickerData
import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StoreQueryParameters
import org.apache.kafka.streams.state.QueryableStoreTypes
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RestController

@RestController
class StockTickerController(
    private val kafkaStreams: KafkaStreams,
) {
    @GetMapping("/stock-ticker/{symbol}")
    fun getStockTicker(
        @PathVariable symbol: String,
    ): StockTickerDataResponse? {
        val store: ReadOnlyKeyValueStore<String, StockTickerData> =
            kafkaStreams.store(
                StoreQueryParameters.fromNameAndType(
                    "stock-ticker-store",
                    QueryableStoreTypes.keyValueStore(),
                ),
            )

        return store[symbol]?.let {
            StockTickerDataResponse(
                symbol = it.symbol,
                price = it.price,
            )
        }
    }
}

data class StockTickerDataResponse(
    val symbol: String,
    val price: Double,
)
