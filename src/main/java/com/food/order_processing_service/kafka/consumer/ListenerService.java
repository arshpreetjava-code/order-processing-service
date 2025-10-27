package com.food.order_processing_service.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.food.order_processing_service.domain.entity.OrderStatus;
import com.food.order_processing_service.kafka.events.OrderEvent;
import com.food.order_processing_service.kafka.events.PaymentEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.food.order_processing_service.kafka.topics.KafkaTopics.*;

@Service
@Slf4j
@RequiredArgsConstructor
public class ListenerService {

    private final ObjectMapper objectMapper;
    private final KafkaTemplate<String, OrderEvent> kafkaTemplate;

    private final Map<String, OrderStatus> orderStatusMap =  new ConcurrentHashMap<>();

    /**
     * Handle ORDER_CREATED events (consumer 1).
     * <p>
     * Deserializes the incoming OrderEvent, stores tracking state and attempts
     * to progress processing if payment is also completed.
     *
     * @param message the raw JSON message from Kafka
     * @param ack     the Kafka acknowledgment to be called after processing
     * @throws JsonProcessingException if deserialization fails
     */
    @KafkaListener(topics = ORDER_CREATED, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCreated(String message, Acknowledgment ack) throws JsonProcessingException {
        OrderEvent orderEvent = objectMapper.readValue(message, OrderEvent.class);
        String orderId = orderEvent.getOrderId();

        OrderStatus status = orderStatusMap.computeIfAbsent(orderId, id -> new OrderStatus());
        status.setOrderEvent(orderEvent);
        log.info("Order received at Sec 21, CHD and sent for processing to  for {}", orderId);

        ack.acknowledge();

        tryProcess(orderId);
    }

    /**
     * Handle ORDER_CREATED events (consumer 2).
     * <p>
     * Duplicate consumer method to simulate multiple processing steps or
     * partitions. Behavior mirrors {@link #onOrderCreated(String, Acknowledgment)}.
     *
     * @param message the raw JSON message from Kafka
     * @param ack     the Kafka acknowledgment to be called after processing
     * @throws JsonProcessingException if deserialization fails
     */
    @KafkaListener(topics = ORDER_CREATED, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCreatedTwo(String message, Acknowledgment ack) throws JsonProcessingException {
        OrderEvent orderEvent = objectMapper.readValue(message, OrderEvent.class);
        String orderId = orderEvent.getOrderId();

        OrderStatus status = orderStatusMap.computeIfAbsent(orderId, id -> new OrderStatus());
        status.setOrderEvent(orderEvent);
        log.info("Order received at Sec 22, CHD and sent for processing to  for {}", orderId);

        ack.acknowledge();

        tryProcess(orderId);
    }

    /**
     * Handle ORDER_CREATED events (consumer 3).
     * <p>
     * Duplicate consumer method to simulate multiple processing steps or
     * partitions. Behavior mirrors {@link #onOrderCreated(String, Acknowledgment)}.
     *
     * @param message the raw JSON message from Kafka
     * @param ack     the Kafka acknowledgment to be called after processing
     * @throws JsonProcessingException if deserialization fails
     */
    @KafkaListener(topics = ORDER_CREATED, groupId = "${spring.kafka.consumer.group-id}")
    public void onOrderCreatedThree(String message, Acknowledgment ack) throws JsonProcessingException {
        OrderEvent orderEvent = objectMapper.readValue(message, OrderEvent.class);
        String orderId = orderEvent.getOrderId();

        OrderStatus status = orderStatusMap.computeIfAbsent(orderId, id -> new OrderStatus());
        status.setOrderEvent(orderEvent);
        log.info("Order received at Sec 23, CHD and sent for processing to  for {}", orderId);

        ack.acknowledge();

        tryProcess(orderId);
    }

    /**
     * Handle PAYMENT_COMPLETED events by marking payment as done and attempting
     * to progress processing once order data is present.
     *
     * @param message the raw JSON message from Kafka
     * @param ack     the Kafka acknowledgment to be called after processing
     * @throws JsonProcessingException if deserialization fails
     */
    @KafkaListener(topics = PAYMENT_COMPLETED, groupId = "${spring.kafka.consumer.group-id}")
    public void onPaymentCompleted(String message, Acknowledgment ack) throws JsonProcessingException {
        PaymentEvent paymentEvent = objectMapper.readValue(message, PaymentEvent.class);
        String orderId = paymentEvent.getOrderId();

        OrderStatus status = orderStatusMap.computeIfAbsent(orderId, id -> new OrderStatus());
        status.setPaymentDone(true);
        log.info("Payment completed for {}", orderId);

        ack.acknowledge();

        tryProcess(orderId);
    }

    /**
     * Attempt to process an order and forward it to the Food Service once both
     * order data and payment are available.
     *
     * @param orderId the id of the order to attempt processing for
     */
    private void tryProcess(String orderId) {
        OrderStatus status = orderStatusMap.get(orderId);
        if (status != null && status.isPaymentDone() && status.getOrderEvent() != null) {
            log.info("Order has been processed and send to Food Service {}", orderId);
            kafkaTemplate.send(ORDER_PROCESSED, status.getOrderEvent());
            orderStatusMap.remove(orderId);
        } else {
            log.info("Order {} not ready yet", orderId);
        }
    }
}
