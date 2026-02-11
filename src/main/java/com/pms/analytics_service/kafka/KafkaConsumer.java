package com.pms.analytics_service.kafka;

import com.google.protobuf.InvalidProtocolBufferException;
import lombok.extern.java.Log;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import patient.event.PatientEvent;

@Slf4j
@Service
public class KafkaConsumer {

    @KafkaListener(topics = "patient",groupId = "analytics-service ")
    public void consumeEvent(byte[] event) throws InvalidProtocolBufferException {
        try {
            PatientEvent patientEvent=PatientEvent.parseFrom(event);
            log.info("performing bbusiness for analytics here");
            System.out.println("performing bbusiness for analytics here");
            log.info("Recevied patient Event: [PatientId={},PatientName={}, PatientEmail={}]",patientEvent.getPatientId(),patientEvent.getName(),patientEvent.getEmail());

        } catch (InvalidProtocolBufferException e) {
            log.error("error deserializing the event {}", e.getMessage());
            e.printStackTrace();
            throw new RuntimeException(e);
        }

    }

}
