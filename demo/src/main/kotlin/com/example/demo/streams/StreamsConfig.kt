package com.example.demo.streams

import org.apache.kafka.streams.KafkaStreams
import org.apache.kafka.streams.StreamsConfig
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration
import org.springframework.kafka.config.KafkaStreamsConfiguration
import org.springframework.kafka.config.StreamsBuilderFactoryBean
import org.springframework.kafka.config.StreamsBuilderFactoryBeanConfigurer

@Configuration
class StreamsConfig {
    @Bean
    fun streamsBuilderFactoryBeanConfigurer(): StreamsBuilderFactoryBeanConfigurer =
        StreamsBuilderFactoryBeanConfigurer { factoryBean ->
            factoryBean.setKafkaStreamsCustomizer { kafkaStreams ->
                kafkaStreams.setStateListener { newState, oldState ->
                    if (newState == KafkaStreams.State.ERROR) {
                        println("Error state transition from $oldState to $newState")
                    }
                }
            }
        }

    @Bean
    fun streamsBuilderFactoryBean(kStreamsConfigs: KafkaStreamsConfiguration): StreamsBuilderFactoryBean =
        StreamsBuilderFactoryBean(kStreamsConfigs).apply {
            isAutoStartup = true
        }

    @Bean(name = [KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME])
    fun kStreamsConfigs(
        @Value("\$spring.kafka.streams.application-id") applicationId: String,
        @Value("\$spring.kafka.streams.bootstrap-servers") bootstrapServers: String,
    ): KafkaStreamsConfiguration =
        KafkaStreamsConfiguration(
            mapOf(
                StreamsConfig.APPLICATION_ID_CONFIG to applicationId,
                StreamsConfig.BOOTSTRAP_SERVERS_CONFIG to bootstrapServers,
            ),
        )
}
