package com.maersk.kafkautility.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.*;

@Slf4j
@Component
public class PayloadWriter<T> {

    public String writePayloadObject(T payload)
    {
        String outputFile = "C:\\Users\\BhavyaSingh\\Documents\\apmm-workspace\\payload.dat";
        try (FileOutputStream fos = new FileOutputStream(outputFile);
             ObjectOutputStream oos = new ObjectOutputStream(fos))
        {
            Class<?> clazz = payload.getClass();
            log.info("Payload class name : {}", clazz.getName());
            oos.writeObject(payload);
            log.info("Payload successfully written in the file");
        } catch (IOException e) {
            log.error("Exception in writePayloadObject", e);
        }
        return outputFile;
    }
}
