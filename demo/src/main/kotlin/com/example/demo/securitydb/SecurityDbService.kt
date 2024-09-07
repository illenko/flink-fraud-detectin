package com.example.demo.securitydb

import com.illenko.avro.Purchase
import org.springframework.stereotype.Service

@Service
class SecurityDbService {
    fun save(purchase: Purchase) {
        println("Saved purchase: $purchase in the security database")
    }
}
