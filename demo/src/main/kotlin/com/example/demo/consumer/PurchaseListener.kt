package com.example.demo.consumer

import com.illenko.avro.PurchaseRecord
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Service

@Service
class PurchaseListener {
    @KafkaListener(topics = ["purchase"], groupId = "purchase-consumer-group")
    fun listen(purchaseRecord: PurchaseRecord) {
        println("Received: $purchaseRecord")
    }
}
