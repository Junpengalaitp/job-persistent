package com.example.job.persistent.service;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;

import java.io.IOException;

import static com.example.job.persistent.config.AmazonS3Config.CRAWLED_JOB_DESCRIPTION_BUCKET;
import static com.example.job.persistent.config.AmazonS3Config.S3_CLIENT;

/**
 * @description: TODO
 * @author: Junpeng He
 * @date: 2021-04-28 3:37 PM
 */
@Slf4j
@Service
public class PersistServiceImpl implements PersistService {

    @Override
    public boolean putToS3(ConsumerRecord<String, String> consumerRecord) throws IOException, InterruptedException {
        String key = consumerRecord.key();
        String value = consumerRecord.value();
        String jobSite = consumerRecord.key().split("-")[0];

        PutObjectResponse putObjectResponse = S3_CLIENT.putObject(PutObjectRequest.builder()
                        .bucket(CRAWLED_JOB_DESCRIPTION_BUCKET)
                        .key(jobSite + "/" + key)
                        .build(),
                RequestBody.fromString(value));
        log.info("put to S3: {}", key);
        return putObjectResponse.sdkHttpResponse().isSuccessful();
    }

}
