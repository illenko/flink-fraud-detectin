package com.example.stocks

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class StocksApplication

fun main(args: Array<String>) {
    runApplication<StocksApplication>(*args)
}
