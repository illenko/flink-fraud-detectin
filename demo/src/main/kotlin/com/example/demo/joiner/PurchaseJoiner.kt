package com.example.demo.joiner

import com.illenko.avro.CorrelatedPurchase
import com.illenko.avro.Purchase
import org.apache.kafka.streams.kstream.ValueJoiner

class PurchaseJoiner : ValueJoiner<Purchase, Purchase, CorrelatedPurchase> {
    override fun apply(
        purchase: Purchase?,
        otherPurchase: Purchase?,
    ): CorrelatedPurchase {
        println("[PurchaseJoiner] Applying joiner with purchase: $purchase and otherPurchase: $otherPurchase")

        val purchaseDate = purchase?.purchaseDate
        val price = purchase?.price ?: 0.0
        val itemPurchased = purchase?.itemPurchased

        val otherPurchaseDate = otherPurchase?.purchaseDate
        val otherPrice = otherPurchase?.price ?: 0.0
        val otherItemPurchased = otherPurchase?.itemPurchased

        println("[PurchaseJoiner] purchaseDate: $purchaseDate, price: $price, itemPurchased: $itemPurchased")
        println("[PurchaseJoiner] otherPurchaseDate: $otherPurchaseDate, otherPrice: $otherPrice, otherItemPurchased: $otherItemPurchased")

        val purchasedItems =
            mutableListOf<String>().apply {
                itemPurchased?.let { add(it) }
                otherItemPurchased?.let { add(it) }
            }

        val customerId = purchase?.customerId
        val otherCustomerId = otherPurchase?.customerId

        println("[PurchaseJoiner] customerId: $customerId, otherCustomerId: $otherCustomerId")
        println("[PurchaseJoiner] purchasedItems: $purchasedItems")

        return CorrelatedPurchase(
            customerId ?: otherCustomerId,
            purchasedItems,
            price + otherPrice,
            purchaseDate,
            otherPurchaseDate,
        ).also {
            println("[PurchaseJoiner] Resulting CorrelatedPurchase: $it")
        }
    }
}
