package com.food.order_processing_service.kafka.topics;

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class KafkaTopicsTest {

    @Test
    void constants_haveExpectedValues() {
        assertThat(KafkaTopics.ORDER_CREATED).isEqualTo("order-created");
        assertThat(KafkaTopics.PAYMENT_COMPLETED).isEqualTo("payment-completed");
        assertThat(KafkaTopics.ORDER_PROCESSED).isEqualTo("order-processed");
    }
}


