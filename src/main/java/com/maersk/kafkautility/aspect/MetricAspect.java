package com.maersk.kafkautility.aspect;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.extern.slf4j.Slf4j;
import org.aspectj.lang.JoinPoint;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.After;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.annotation.Pointcut;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;

@Aspect
@Slf4j
@Configuration
public class MetricAspect {

    @Autowired
    private MeterRegistry meterRegistry;

    @Pointcut("@annotation(com.maersk.kafkautility.annotations.CounterMetric)")
    public void annotatedMethod(){
    }

    @After(value = "annotatedMethod()")
    public void metricAdvice(JoinPoint joinPoint)
    {
        try {
            var className = joinPoint.getSignature().getDeclaringType().getName();
            log.info("Class name: {}", className);
            var methodName = joinPoint.getSignature().getName();
            Counter counter = initializeMethodMetric(className, methodName);
            counter.increment();
        } catch (Exception t)
        {
            log.error("Exception thrown by intercepted method", t);
            exceptionCounterMetric(t);
        }
    }

    private Counter initializeMethodMetric(String className, String methodName)
    {
        log.info("Inside initializeMethodMetric");
        return Counter.builder(className.concat(".").concat(methodName))
                .tag("Method", className.concat(".").concat(methodName))
                .description("Number of times consumer method is invoked")
                .register(meterRegistry);
    }

    private void exceptionCounterMetric(Exception exception)
    {
        log.info("Inside exceptionCounterMetric");
        Counter counter = Counter.builder(exception.getClass().getName())
                .tag("Exception", exception.getClass().getName())
                .description("Exception counter")
                .register(meterRegistry);
        counter.increment();
    }
}
