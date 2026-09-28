package in.codefarm.order.service.as.producer.config;

import jakarta.persistence.EntityManagerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.transaction.KafkaTransactionManager;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.transaction.annotation.EnableTransactionManagement;

@Configuration
@EnableTransactionManagement
public class KafkaTransactionConfig {

    // Defining kafkaTransactionManager makes Spring Boot back off from creating the default
    // JPA "transactionManager", which plain @Transactional (OrderEventProducerService) needs
    @Bean
    @Primary
    public JpaTransactionManager transactionManager(EntityManagerFactory entityManagerFactory) {
        return new JpaTransactionManager(entityManagerFactory);
    }

    @Bean
    public KafkaTransactionManager<String, Object> kafkaTransactionManager(
        ProducerFactory<String, Object> transactionalProducerFactory
    ) {
        return new KafkaTransactionManager<>(transactionalProducerFactory);
    }
}

