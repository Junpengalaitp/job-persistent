package com.example.job.persistent.message;

import com.example.job.persistent.config.TopicName;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @description: TODO
 * @author: Junpeng He
 * @date: 2021-04-29 10:12 AM
 */
@Component
public class SparkKafkaConsumer {
    @Value("${spring.kafka.consumer.bootstrap-servers}")
    private String bootstrapServers;

    private static final SparkConf conf = new SparkConf()
            .setAppName("startingSpark")
            .setMaster("local[*]");

    private static final JavaStreamingContext streamingContext = new JavaStreamingContext(conf, new Duration(5000));

    @Autowired
    private ConsumerFactory<Object, Object> consumerFactory;

    @EventListener(ApplicationReadyEvent.class)
    public void startSparkKafka() throws InterruptedException {
        Map<String, Object> params = new HashMap<>(consumerFactory.getConfigurationProperties());
        params.put("bootstrap.servers", bootstrapServers);
        JavaInputDStream<ConsumerRecord<String, String>> stream = KafkaUtils.createDirectStream(streamingContext,
                LocationStrategies.PreferConsistent(), ConsumerStrategies.Subscribe(List.of(TopicName.CRAWLED_JOB_DESCRIPTION), params));

        stream.map(ConsumerRecord::value)
                .foreachRDD(rdd -> rdd.saveAsTextFile("hdfs://localhost:8020/job-info/dice"));
        streamingContext.start();
        streamingContext.awaitTermination();
    }
}
