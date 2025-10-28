package com.food.order_processing_service.kafka.consumer;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.food.order_processing_service.kafka.events.OrderEvent;
import com.food.order_processing_service.kafka.events.PaymentEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;

import java.time.LocalDateTime;

import static com.food.order_processing_service.kafka.topics.KafkaTopics.ORDER_PROCESSED;
import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

class ListenerServiceTest {

    private ObjectMapper objectMapper;
    private KafkaTemplate<String, OrderEvent> kafkaTemplate;
    private ListenerService listenerService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        @SuppressWarnings("unchecked")
        KafkaTemplate<String, OrderEvent> mocked = (KafkaTemplate<String, OrderEvent>) mock(KafkaTemplate.class);
        kafkaTemplate = mocked;
        listenerService = new ListenerService(objectMapper, kafkaTemplate);
    }

    @Test
    void onOrderCreated_NotReadyYet_DoesNotProduce() throws Exception {
        Acknowledgment ack = mock(Acknowledgment.class);
        OrderEvent orderEvent = OrderEvent.builder()
                .orderId("o-1")
                .userId("u-1")
                .address("addr")
                .food(null)
                .totalAmount(100)
                .build();

        String json = objectMapper.writeValueAsString(orderEvent);

        listenerService.onOrderCreated(json, ack);

        verify(ack, times(1)).acknowledge();
        verify(kafkaTemplate, never()).send(anyString(), any(OrderEvent.class));
    }

    @Test
    void onPaymentCompleted_NotReadyYet_DoesNotProduce() throws Exception {
        Acknowledgment ack = mock(Acknowledgment.class);
        PaymentEvent paymentEvent = PaymentEvent.builder()
                .orderId("o-2")
                .userId("u-2")
                .status("PAID")
                .amount(200.0)
                .build();

        String json = objectMapper.writeValueAsString(paymentEvent);

        listenerService.onPaymentCompleted(json, ack);

        verify(ack, times(1)).acknowledge();
        verify(kafkaTemplate, never()).send(anyString(), any(OrderEvent.class));
    }

    @Test
    void orderThenPayment_ProducesAndClearsState() throws Exception {
        String orderId = "o-3";

        Acknowledgment ack1 = mock(Acknowledgment.class);
        Acknowledgment ack2 = mock(Acknowledgment.class);

        OrderEvent orderEvent = OrderEvent.builder()
                .orderId(orderId)
                .userId("u-3")
                .address("addr")
                .food(null)
                .totalAmount(300)
                .build();
        String orderJson = objectMapper.writeValueAsString(orderEvent);

        PaymentEvent paymentEvent = PaymentEvent.builder()
                .orderId(orderId)
                .userId("u-3")
                .status("PAID")
                .amount(300.0)
                .build();
        String paymentJson = objectMapper.writeValueAsString(paymentEvent);

        listenerService.onOrderCreated(orderJson, ack1);
        listenerService.onPaymentCompleted(paymentJson, ack2);

        verify(ack1).acknowledge();
        verify(ack2).acknowledge();

        ArgumentCaptor<OrderEvent> eventCaptor = ArgumentCaptor.forClass(OrderEvent.class);
        verify(kafkaTemplate, times(1)).send(eq(ORDER_PROCESSED), eventCaptor.capture());
        assertThat(eventCaptor.getValue().getOrderId()).isEqualTo(orderId);
    }

    @Test
    void orderAcrossMultipleConsumers_StillProcessesOnce() throws Exception {
        String orderId = "o-4";
        Acknowledgment ack1 = mock(Acknowledgment.class);
        Acknowledgment ack2 = mock(Acknowledgment.class);
        Acknowledgment ack3 = mock(Acknowledgment.class);
        Acknowledgment ackPay = mock(Acknowledgment.class);

        OrderEvent orderEvent = OrderEvent.builder()
                .orderId(orderId)
                .userId("u-4")
                .address("addr")
                .food(null)
                .totalAmount(400)
                .build();
        String orderJson = objectMapper.writeValueAsString(orderEvent);

        PaymentEvent paymentEvent = PaymentEvent.builder()
                .orderId(orderId)
                .userId("u-4")
                .status("PAID")
                .amount(400.0)
                .build();
        String paymentJson = objectMapper.writeValueAsString(paymentEvent);

        listenerService.onOrderCreated(orderJson, ack1);
        listenerService.onOrderCreatedTwo(orderJson, ack2);
        listenerService.onOrderCreatedThree(orderJson, ack3);
        listenerService.onPaymentCompleted(paymentJson, ackPay);

        verify(ack1).acknowledge();
        verify(ack2).acknowledge();
        verify(ack3).acknowledge();
        verify(ackPay).acknowledge();

        // Only one send despite multiple consumers updating same orderId
        verify(kafkaTemplate, times(1)).send(eq(ORDER_PROCESSED), any(OrderEvent.class));
    }
}


