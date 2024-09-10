package com.illenko.producer

import com.illenko.avro.Purchase
import com.illenko.avro.StockTickerData
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.KafkaTemplate
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

@SpringBootApplication
class ProducerApplication {
    @Bean
    fun run(kafkaTemplate: KafkaTemplate<String, Purchase>) =
        CommandLineRunner {
            val scheduler = Executors.newScheduledThreadPool(1)
            scheduler.scheduleAtFixedRate({
                val purchaseRecord = generateRandomPurchaseRecord()
                kafkaTemplate.send("purchase", UUID.randomUUID().toString(), purchaseRecord).get()
                println("Sent: $purchaseRecord")
            }, 0, 10, TimeUnit.SECONDS)
        }

    private fun generateRandomPurchaseRecord(): Purchase {
        val firstNames = listOf("John", "Jane", "Alex", "Chris")
        val lastNames = listOf("Doe", "Smith", "Johnson", "Brown")
        val customerIds = listOf("1001", "1002", "1003", "1004")
        val creditCardNumbers = listOf("4111111111111111", "4111111111112222", "4111111111113333", "4111111111114444")
        val itemsPurchased = listOf("Item1", "Item2", "Item3", "Item4")
        val departments = listOf("coffee", "electronics", "grocery", "water")
        val employeeIds = listOf("E100", "E200", "E300", "E400")
        val quantities = listOf(1, 2, 3, 4)
        val prices = listOf(10.0, 20.0, 30.0, 40.0)
        val zipCodes = listOf("10000", "20000", "30000", "40000")
        val storeIds = listOf("S1", "S2", "S3", "S4")

        return Purchase
            .newBuilder()
            .setFirstName(firstNames.random())
            .setLastName(lastNames.random())
            .setCustomerId(customerIds.random())
            .setCreditCardNumber(creditCardNumbers.random())
            .setItemPurchased(itemsPurchased.random())
            .setDepartment(departments.random())
            .setEmployeeId(employeeIds.random())
            .setQuantity(quantities.random())
            .setPrice(prices.random())
            .setPurchaseDate(Instant.now())
            .setZipCode(zipCodes.random())
            .setStoreId(storeIds.random())
            .build()
    }

    @Bean
    fun produceStocks(kafkaTemplate: KafkaTemplate<String, StockTickerData>) =
        CommandLineRunner {
            val scheduler = Executors.newScheduledThreadPool(1)
            scheduler.scheduleAtFixedRate({
                val stockRecord = generateRandomStock()
                kafkaTemplate.send("stock-tickers", stockRecord.symbol, stockRecord).get()
                println("Sent: $stockRecord")
            }, 0, 10, TimeUnit.SECONDS)
        }

    private fun generateRandomStock(): StockTickerData {
        val stockTickers = listOf("AAPL", "GOOGL", "AMZN", "MSFT")
        val prices = listOf(10.0, 20.0, 30.0, 40.0, 50.0, 60.0, 70.0, 80.0, 90.0, 100.0)

        return StockTickerData
            .newBuilder()
            .setSymbol(stockTickers.random())
            .setPrice(prices.random())
            .build()
    }
}

fun main(args: Array<String>) {
    runApplication<ProducerApplication>(*args)
}
