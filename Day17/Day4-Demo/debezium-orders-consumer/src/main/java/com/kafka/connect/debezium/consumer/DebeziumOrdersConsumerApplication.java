package com.kafka.connect.debezium.consumer;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class DebeziumOrdersConsumerApplication {

	public static void main(String[] args) {
		SpringApplication.run(DebeziumOrdersConsumerApplication.class, args);
	}
}

