package com.lta.backend.listeners;

import lombok.extern.log4j.Log4j2;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Log4j2
@Component
public class StrConsumerListener {

    @KafkaListener(groupId = "group-0", topics = "str-topic", containerFactory = "strContainerFactory")
    public void listener1(String message){
        log.info("Recibiendo un mensaje {} ", message);
        log.info("LISTENER ::: Recibiendo un mensaje {} ", message);
    }

    @KafkaListener(groupId = "group-1", topics = "str-topic", containerFactory = "strContainerFactory")
    public void listener2(String message){
        log.info("Recibiendo un mensaje {} ", message);
        log.info("LISTENER2 ::: Recibiendo un mensaje {} ", message);
    }

    @KafkaListener(groupId = "group-1", topics = "str-topic", containerFactory = "strContainerFactory")
    public void listener3(String message){
        log.info("Recibiendo un mensaje {} ", message);
        log.info("LISTENER3 ::: Recibiendo un mensaje {} ", message);
    }

    @KafkaListener(groupId = "group-2", topics = "str-topic", containerFactory = "strContainerFactory")
    public void listene4(String message){
        log.info("Recibiendo un mensaje {} ", message);
        log.info("LISTENER4 ::: Recibiendo un mensaje {} ", message);
    }


}
