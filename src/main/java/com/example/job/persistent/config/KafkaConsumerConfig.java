package com.example.job.persistent.config;

import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.dao.RecoverableDataAccessException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.retry.RetryPolicy;
import org.springframework.retry.backoff.FixedBackOffPolicy;
import org.springframework.retry.policy.SimpleRetryPolicy;
import org.springframework.retry.support.RetryTemplate;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * @description: TODO
 * @author: Junpeng He
 * @date: 2021-04-26 3:58 PM
 */
@Slf4j
@Configuration
@EnableKafka
public class KafkaConsumerConfig {

    @Bean
    @ConditionalOnMissingBean(name = "kafkaListenerContainerFactory")
    ConcurrentKafkaListenerContainerFactory<Object, Object> kafkaListenerContainerFactory(
            ConcurrentKafkaListenerContainerFactoryConfigurer concurrentKafkaListenerContainerFactoryConfigurer,
            ConsumerFactory<Object, Object> consumerFactory) {
        ConcurrentKafkaListenerContainerFactory<Object, Object> concurrentKafkaListenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        concurrentKafkaListenerContainerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.BATCH);
        concurrentKafkaListenerContainerFactory.setConcurrency(3);
        concurrentKafkaListenerContainerFactory.setRetryTemplate(retryTemplate());
        concurrentKafkaListenerContainerFactory.setRecoveryCallback(context -> {
            if (context.getLastThrowable().getCause() instanceof RecoverableDataAccessException) {
                log.info("Inside the recoverable exception");
                Arrays.stream(context.attributeNames()).forEach(name -> {
                    log.info("Attribute name is: {}", name);
                    log.info("Attribute value is: {}", context.getAttribute(name));
                });
            } else {
                log.info("Inside the non-recoverable exception");
                throw new RuntimeException(context.getLastThrowable().getMessage());
            }
            return null;
        });
        concurrentKafkaListenerContainerFactoryConfigurer.configure(concurrentKafkaListenerContainerFactory, consumerFactory);
        return concurrentKafkaListenerContainerFactory;
    }

    private RetryTemplate retryTemplate() {
        FixedBackOffPolicy fixedBackOffPolicy = new FixedBackOffPolicy();
        fixedBackOffPolicy.setBackOffPeriod(1000);
        RetryTemplate retryTemplate = new RetryTemplate();
        retryTemplate.setRetryPolicy(simpleRetryPolice());
        retryTemplate.setBackOffPolicy(fixedBackOffPolicy);
        return retryTemplate;
    }

    private RetryPolicy simpleRetryPolice() {
        Map<Class<? extends Throwable>, Boolean> exceptionsMap = new HashMap<>();
        exceptionsMap.put(Exception.class, true);
        exceptionsMap.put(RecoverableDataAccessException.class, false);
        return new SimpleRetryPolicy(3, exceptionsMap, true);
    }
}
