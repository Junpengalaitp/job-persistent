package com.example.job.persistent.service;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.IOException;

/**
 * @description: TODO
 * @author: Junpeng He
 * @date: 2021-04-28 3:34 PM
 */
public interface PersistService {

    boolean putToS3(ConsumerRecord<String, String> consumerRecord) throws IOException, InterruptedException;
}
