package in.codefarm.notification.service.as.consumer.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConsumerConfig {

    // Base consumer factory configuration
    private Map<String, Object> baseConsumerConfigs() {
        Map<String, Object> configProps = new HashMap<>();

        configProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:19092,localhost:29092,localhost:39092");
        configProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        configProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);

       // Auto offset reset
        configProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Heartbeat interval
        configProps.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 3000);

        // Max poll records
        configProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 50);

        // Max poll interval
        configProps.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);

//        configProps.put(ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, List.of(CooperativeStickyAssignor.class)); //todo

        return configProps;
    }

    // Consumer factory for auto-commit (default)
    @Bean
    public ConsumerFactory<String, String> autoCommitConsumerFactory() {
        Map<String, Object> configProps = baseConsumerConfigs();

        // Auto commit configuration
        configProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
//        configProps.put(CO, false);
//        configProps.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);//

        configProps.put(ConsumerConfig.GROUP_ID_CONFIG, "notification-service-group");

        return new DefaultKafkaConsumerFactory<>(configProps);
    }

    // Container factory for auto-commit
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(
            ConsumerFactory<String, String> autoCommitConsumerFactory
    ) {
        ConcurrentKafkaListenerContainerFactory<String, String> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(autoCommitConsumerFactory);
        factory.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        factory.setConcurrency(3); // 3 consumer threads
        return factory;
    }
}

