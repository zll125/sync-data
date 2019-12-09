package com.mikasaco.syncdata.kafka;

import com.mikasaco.syncdata.elasticsearch.EsClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;

@Slf4j
public class CanalListener {

    @Autowired
    private EsClient esClient;

    @KafkaListener(topics = "syncdata")
    public void listenCaseInfo(String message) {
        log.info("接收到的消息: " + message);

    }
}
