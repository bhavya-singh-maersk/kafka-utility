package com.maersk.kafkautility.aspect;

import com.maersk.kafkautility.service.MessagePublishHandler;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Configurable;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;
import org.springframework.core.annotation.Order;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.retry.support.RetrySynchronizationManager;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

@Slf4j
@Order(0)
@Aspect
@Configuration
public class RetryAspect<T> {

	@Autowired
	private ApplicationContext context;

	@Autowired
	private KafkaTemplate<String, T> kafkaTemplate;

	@Autowired
	private MessagePublishHandler<T> messagePublishHandler;

	private static final String RETRY_TOPIC_PLACEHOLDER = "${kafka.retry.topic}";

	@Pointcut("@annotation(com.maersk.kafkautility.annotations.RetryHandler)")
	public void annotatedMethod(){
	}

	/*//@After(value = "annotatedMethod()")
	public void retryAdvice(JoinPoint joinPoint) {
		log.info("Inside retryAdvice");
		try {
			var retryTopic = context.getEnvironment().resolvePlaceholders(RETRY_TOPIC_PLACEHOLDER);
			log.info("retryTopic: {}", retryTopic);
			var args = joinPoint.getArgs();
			if (Objects.nonNull(args[0])) {
				var message = (T) args[0];
				log.info("message: {}", message);
				ProducerRecord<String, T> producerRecord = new ProducerRecord<>(retryTopic, message);
				var kafkaHeaders = producerRecord.headers();
				if (Objects.nonNull(args[1])) {
					kafkaHeaders.add("X-DOCBROKER-Correlation-ID", args[1].toString().getBytes(StandardCharsets.UTF_8));
				}
				messagePublishHandler.publishOnTopic(producerRecord);
			}
		} catch (Exception ex)
		{
			log.error("Exception in retry advice ", ex);
		}
	}*/

	@AfterThrowing(value = "annotatedMethod()")
	public void retryAdvice(JoinPoint joinPoint) {
		log.info("Retry threshold reached - inside retryAdvice");
		var retryTopic = context.getEnvironment().resolvePlaceholders(RETRY_TOPIC_PLACEHOLDER);
		log.info("retry topic name: {}", retryTopic);
		var args = joinPoint.getArgs();
		if (Objects.nonNull(args[0])) {
			var message = (T) args[0];
			log.info("message to publish on retry topic: {}", message);
			ProducerRecord<String, T> producerRecord = new ProducerRecord<>(retryTopic, message);
			var kafkaHeaders = producerRecord.headers();
			if (Objects.nonNull(args[1])) {
				kafkaHeaders.add("X-DOCBROKER-Correlation-ID", args[1].toString().getBytes(StandardCharsets.UTF_8));
			}
			messagePublishHandler.publishOnTopic(producerRecord);
		}
	}

	@Around(value = "annotatedMethod()")
	public void exceptionHandlerAdvice(ProceedingJoinPoint joinPoint) {
		log.info("Inside exceptionHandlerAdvice");
		try{
			joinPoint.proceed();
		}
		catch (Throwable throwable) {
			log.error("Exception thrown by the intercepted method", throwable);
		}
	}

}
