package com.food.order_processing_service.domain.entity;

import com.food.order_processing_service.kafka.events.OrderEvent;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class OrderStatus {
    private OrderEvent orderEvent;
    private boolean paymentDone = false;
}