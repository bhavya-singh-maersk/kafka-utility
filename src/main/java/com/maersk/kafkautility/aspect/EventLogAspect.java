package com.maersk.kafkautility.aspect;

import com.maersk.kafkautility.service.AuditService;
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
public class EventLogAspect {

    @Autowired
    private AuditService auditService;

    @Pointcut("@annotation(com.maersk.kafkautility.annotations.LogEvent)")
    public void annotatedMethod(){
    }

    @Around(value = "annotatedMethod()")
    public void eventAdvice(ProceedingJoinPoint joinPoint) {
        log.info("Inside eventAdvice");
        try{
            var className = joinPoint.getSignature().getDeclaringType().getName();
            var methodName = joinPoint.getSignature().getName();
            var args = joinPoint.getArgs();
            var payload = args[0];
           // auditService.logEvent(className, methodName, payload.toString());
            joinPoint.proceed();
            log.info("After proceed");
        }
        catch (Throwable throwable) {
            log.error("Exception thrown by the intercepted method", throwable);
        }
    }

   // @After(value = "annotatedMethod()")
    public void afterEventAdvice(JoinPoint joinPoint) {
        log.info("Inside afterEventAdvice");
        log.info(joinPoint.getSignature().getName());
    }
}
