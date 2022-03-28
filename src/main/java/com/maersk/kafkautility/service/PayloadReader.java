package com.maersk.kafkautility.service;

import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.Paths;

@Slf4j
@Component
public class PayloadReader<T> {

    public T readPayloadObject(String claimCheckKey)
    {
        Object obj = null;
        try (FileInputStream fis = new FileInputStream(String.valueOf(Paths.get(claimCheckKey)));
             ObjectInputStream ois = new ObjectInputStream(fis))
        {
            obj = ois.readObject();
            Class<?> clazz = obj.getClass();
            log.info("Payload class name : {}", clazz.getName());
        } catch (IOException | ClassNotFoundException e) {
            log.error("Exception in readPayloadObject ", e);
        }
        return (T) obj;
    }
}
