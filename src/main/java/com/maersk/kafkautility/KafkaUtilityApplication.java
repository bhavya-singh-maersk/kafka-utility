package com.maersk.kafkautility;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

@SpringBootApplication
@EnableAspectJAutoProxy
public class KafkaUtilityApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaUtilityApplication.class, args);
	}

}
