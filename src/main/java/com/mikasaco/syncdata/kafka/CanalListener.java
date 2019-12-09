package com.mikasaco.syncdata.kafka;

import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;

@Slf4j
public class CanalListener {

    @KafkaListener(topics = "syncdata")
    public void listenCaseInfo(String message) {
        log.info("接收到的消息: " + message);

    }
}
