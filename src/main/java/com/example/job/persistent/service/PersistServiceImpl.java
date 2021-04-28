package com.example.job.persistent.service;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.example.job.persistent.hdfs.HadoopPersistent;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.stereotype.Service;

import java.io.FileWriter;
import java.io.IOException;

/**
 * @description: TODO
 * @author: Junpeng He
 * @date: 2021-04-28 3:37 PM
 */
@Slf4j
@Service
public class PersistServiceImpl implements PersistService {

    @Override
    public String getJobInfoString(ConsumerRecord<String, String> consumerRecord) throws IOException, InterruptedException {
        String value = consumerRecord.value();
        JSONObject jsonObject = JSON.parseObject(value);
        String jobInfoString = jsonObject.getString("jobInfo");
        String fileName = "job_info_" + System.currentTimeMillis() + ".txt";
        String path = "/Users/hejunpeng/alaitp/job-persistent/src/main/resources/job-info/" + fileName;
        saveAsFileWriter(jobInfoString, path);
        HadoopPersistent.storeToHdfs(path, fileName);
        log.info(jobInfoString);
        return jobInfoString;
    }

    private static void saveAsFileWriter(String content, String path) {

        FileWriter fwriter = null;
        try {
            fwriter = new FileWriter(path);
            fwriter.write(content);
        } catch (IOException ex) {
            ex.printStackTrace();
        } finally {
            try {
                fwriter.flush();
                fwriter.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }
    }
}
