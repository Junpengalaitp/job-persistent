package com.example.job.persistent.hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;

/**
 * @description: TODO
 * @author: Junpeng He
 * @date: 2021-04-28 2:57 PM
 */
public class HadoopPersistent {
    public static final Configuration configuration = new Configuration();

    public static void storeToHdfs(String localPathStr, String fileName) throws IOException, InterruptedException {
        FileSystem fs = FileSystem.get(URI.create("hdfs://localhost:8020"), configuration ,"hejunpeng");
        Path localPath = new Path(localPathStr);
        Path hdfsPath = new Path("hdfs://localhost:8020/input/" + fileName);
        fs.copyFromLocalFile(localPath, hdfsPath);
        fs.close();
    }
}
