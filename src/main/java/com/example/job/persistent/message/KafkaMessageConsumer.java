package com.example.job.persistent.message;

import com.example.job.persistent.config.TopicName;
import com.example.job.persistent.service.PersistService;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.io.IOException;

/**
 * @description: TODO
 * @author: Junpeng He
 * @date: 2021-04-26 4:07 PM
// */
@Component
@Slf4j
public class KafkaMessageConsumer {

    @Autowired
    private PersistService persistService;

    @KafkaListener(topics = {TopicName.CRAWLED_JOB_DESCRIPTION})
    private void onMessage(ConsumerRecord<String, String> consumerRecord) throws IOException, InterruptedException {
        log.info("ConsumerRecord: {}", consumerRecord);
        persistService.putToS3(consumerRecord);
    }
}
