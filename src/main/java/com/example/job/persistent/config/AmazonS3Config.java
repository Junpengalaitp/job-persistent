package com.example.job.persistent.config;

import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

/**
 * @description: TODO
 * @author: Junpeng He
 * @date: 2021-05-03 8:59 AM
 */
public class AmazonS3Config {
    public static final Region region = Region.AP_SOUTHEAST_1;
    public static final S3Client S3_CLIENT = S3Client.builder().region(region).build();
    public static final String CRAWLED_JOB_DESCRIPTION_BUCKET = TopicName.CRAWLED_JOB_DESCRIPTION;
}
