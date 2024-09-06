package com.illenko.producer

import com.illenko.avro.PurchaseRecord
import org.springframework.boot.CommandLineRunner
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.kafka.core.KafkaTemplate
import java.time.Instant
import java.util.UUID
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.random.Random

@SpringBootApplication
class ProducerApplication {
    @Bean
    fun run(kafkaTemplate: KafkaTemplate<String, PurchaseRecord>) =
        CommandLineRunner {
            val scheduler = Executors.newScheduledThreadPool(1)
            scheduler.scheduleAtFixedRate({
                val purchaseRecord = generateRandomPurchaseRecord()
                kafkaTemplate.send("purchase", UUID.randomUUID().toString(), purchaseRecord).get()
                println("Sent: $purchaseRecord")
            }, 0, 10, TimeUnit.SECONDS)
        }

    private fun generateRandomPurchaseRecord(): PurchaseRecord =
        PurchaseRecord
            .newBuilder()
            .setFirstName("John${Random.nextInt(1000)}")
            .setLastName("Doe${Random.nextInt(1000)}")
            .setCustomerId(Random.nextInt(10000).toString())
            .setCreditCardNumber("411111111111${Random.nextInt(1000, 9999)}")
            .setItemPurchased("Item${Random.nextInt(100)}")
            .setDepartment("Department${Random.nextInt(10)}")
            .setEmployeeId("E${Random.nextInt(1000)}")
            .setQuantity(Random.nextInt(1, 10))
            .setPrice(Random.nextDouble(10.0, 1000.0))
            .setPurchaseDate(Instant.now())
            .setZipCode(Random.nextInt(10000, 99999).toString())
            .setStoreId("S${Random.nextInt(100)}")
            .build()
}

fun main(args: Array<String>) {
    runApplication<ProducerApplication>(*args)
}
