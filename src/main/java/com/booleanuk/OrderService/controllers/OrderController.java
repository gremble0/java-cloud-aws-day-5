package com.booleanuk.OrderService.controllers;


import com.booleanuk.OrderService.models.Order;
import com.booleanuk.OrderService.repositories.OrderRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.web.server.ResponseStatusException;
import software.amazon.awssdk.services.eventbridge.EventBridgeClient;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequest;
import software.amazon.awssdk.services.eventbridge.model.PutEventsRequestEntry;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("orders")
public class OrderController {
    private final SqsClient sqsClient;
    private final SnsClient snsClient;
    private final EventBridgeClient eventBridgeClient;
    private final ObjectMapper objectMapper;
    private final OrderRepository repository;
    private final String queueUrl;
    private final String topicArn;
    private final String eventBusName;

    public OrderController(OrderRepository repository) {
        this.sqsClient = SqsClient.builder().build();
        this.snsClient = SnsClient.builder().build();
        this.eventBridgeClient = EventBridgeClient.builder().build();

        this.queueUrl = "";
        this.topicArn = "";
        this.eventBusName = "";

        this.objectMapper = new ObjectMapper();

        this.repository = repository;
    }

    @GetMapping
    public ResponseEntity<List<Order>> GetAllOrders() {
        ReceiveMessageRequest receiveRequest = ReceiveMessageRequest.builder()
                .queueUrl(queueUrl)
                .maxNumberOfMessages(10)
                .waitTimeSeconds(20)
                .build();

        List<Message> messages = sqsClient.receiveMessage(receiveRequest).messages();
        List<Order> orders = new ArrayList<>();

        for (Message message : messages) {
            try {
                orders.add(this.objectMapper.readValue(message.body(), Order.class));

                DeleteMessageRequest deleteRequest = DeleteMessageRequest.builder()
                        .queueUrl(queueUrl)
                        .receiptHandle(message.receiptHandle())
                        .build();

                sqsClient.deleteMessage(deleteRequest);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }

        return ResponseEntity.ok(orders);
    }

    @PostMapping
    public ResponseEntity<Order> createOrder(@RequestBody Order order) {
        try {
            String orderJson = objectMapper.writeValueAsString(order);
            System.out.println(orderJson);
            PublishRequest publishRequest = PublishRequest.builder()
                    .topicArn(topicArn)
                    .message(orderJson)
                    .build();
            snsClient.publish(publishRequest);

            PutEventsRequestEntry eventEntry = PutEventsRequestEntry.builder()
                    .source("order.service")
                    .detailType("OrderCreated")
                    .detail(orderJson)
                    .eventBusName(eventBusName)
                    .build();

            PutEventsRequest putEventsRequest = PutEventsRequest.builder()
                    .entries(eventEntry)
                    .build();

            this.eventBridgeClient.putEvents(putEventsRequest);

            return ResponseEntity.ok(this.repository.save(order));
        } catch (JsonProcessingException e) {
            e.printStackTrace();
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST);
        }
    }
}