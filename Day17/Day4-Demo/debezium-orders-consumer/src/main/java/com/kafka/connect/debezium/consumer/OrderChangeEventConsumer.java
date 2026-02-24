package com.kafka.connect.debezium.consumer;

import org.apache.avro.generic.GenericRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Minimal consumer for Debezium change events from orders table
 * 
 * Consumes from: mysql-server.ecommerce_db.orders
 * 
 * With unwrap transform, messages are Avro GenericRecord format.
 * The record contains:
 * - order_id, customer_id, product_id, quantity, price, order_status
 * - created_at, updated_at
 * - __op (operation: c=create, u=update, d=delete, r=read/snapshot)
 * - __source_ts_ms (timestamp)
 */
@Component
public class OrderChangeEventConsumer {
    
    private static final Logger log = LoggerFactory.getLogger(OrderChangeEventConsumer.class);
    
    @KafkaListener(
        topics = "mysql-server.ecommerce_db.orders",
        groupId = "debezium-orders-consumer-group1",
        containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(GenericRecord record) {
        try {
            // Handle tombstone messages (null records from delete operations)
            if (record == null) {
                log.info("Received tombstone message (null record) - skipping");
                return;
            }
            
            log.info("record - {}", record);
            // Extract operation type (c=create, u=update, d=delete, r=read/snapshot)
            String op = record.get("__op") != null ? record.get("__op").toString() : "unknown";
            
            // Process based on operation type
            switch (op) {
                case "c":
                    handleOrderCreated(record);
                    break;
                case "u":
                    handleOrderUpdated(record);
                    break;
                case "d":
                    handleOrderDeleted(record);
                    break;
                case "r":
                    handleOrderSnapshot(record);
                    break;
                default:
                    log.warn("Unknown operation type: {}", op);
            }
            
        } catch (Exception e) {
            log.error("Error processing message: {}", record, e);
        }
    }
    
    private void handleOrderCreated(GenericRecord order) {
        log.info("🆕 New order created - Order ID: {}, Customer: {}, Status: {}", 
            order.get("order_id"),
            order.get("customer_id"),
            order.get("order_status"));
    }
    
    private void handleOrderUpdated(GenericRecord order) {
        log.info("🔄 Order updated - Order ID: {}, New Status: {}", 
            order.get("order_id"),
            order.get("order_status"));
    }
    
    private void handleOrderDeleted(GenericRecord order) {
        log.info("🗑️  Order deleted - Order ID: {}", 
            order.get("order_id"));
    }
    
    private void handleOrderSnapshot(GenericRecord order) {
        log.info("📸 Snapshot read (initial load) - Order ID: {}, Customer: {}, Status: {}", 
            order.get("order_id"),
            order.get("customer_id"),
            order.get("order_status"));
    }
    
    private String getOperationName(String op) {
        return switch (op) {
            case "c" -> "CREATE";
            case "u" -> "UPDATE";
            case "d" -> "DELETE";
            case "r" -> "READ (Snapshot)";
            default -> "UNKNOWN";
        };
    }
}

