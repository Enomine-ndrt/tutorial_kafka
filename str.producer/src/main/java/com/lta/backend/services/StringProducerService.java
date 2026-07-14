package com.lta.backend.services;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.util.concurrent.ThreadLocalRandom;

@Service
@Slf4j
public class StringProducerService {

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    public void sendMessage(String message){
        int partition = ThreadLocalRandom.current().nextInt(2);

        log.info("PARTICION ELEGIDA: {}", partition);

        kafkaTemplate.send("str-topic",partition,null, message).whenComplete((result, ex) -> {

            if(ex != null){
                log.error("Error , al enviar el mensaje: {}", ex.getMessage());
            }
                log.info("Mensaje enviado con exito: {}",result.getProducerRecord().value());
                log.info("Particion {}, Offset {}",result.getRecordMetadata().partition(),
                        result.getRecordMetadata().offset());
        });
    }
}
