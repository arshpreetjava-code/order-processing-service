package com.food.order_processing_service.kafka.events;

import com.food.order_processing_service.domain.dto.FoodDto;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;

import static org.assertj.core.api.Assertions.assertThat;

class OrderAndPaymentEventTest {

    @Test
    void orderEvent_builderAndAccessorsWork() {
        FoodDto food = FoodDto.builder()
                .type("pizza")
                .name("margherita")
                .toppings(new String[]{"basil"})
                .quantity(1)
                .price(499)
                .build();

        LocalDateTime now = LocalDateTime.now();
        OrderEvent event = OrderEvent.builder()
                .orderId("o-10")
                .userId("u-10")
                .address("addr")
                .food(food)
                .totalAmount(499)
                .createTime(now)
                .build();

        assertThat(event.getOrderId()).isEqualTo("o-10");
        assertThat(event.getUserId()).isEqualTo("u-10");
        assertThat(event.getAddress()).isEqualTo("addr");
        assertThat(event.getFood()).isSameAs(food);
        assertThat(event.getTotalAmount()).isEqualTo(499);
        assertThat(event.getCreateTime()).isEqualTo(now);
    }

    @Test
    void paymentEvent_builderAndAccessorsWork() {
        PaymentEvent ev = PaymentEvent.builder()
                .orderId("o-11")
                .userId("u-11")
                .status("PAID")
                .amount(100.0)
                .build();

        assertThat(ev.getOrderId()).isEqualTo("o-11");
        assertThat(ev.getUserId()).isEqualTo("u-11");
        assertThat(ev.getStatus()).isEqualTo("PAID");
        assertThat(ev.getAmount()).isEqualTo(100.0);
    }
}


