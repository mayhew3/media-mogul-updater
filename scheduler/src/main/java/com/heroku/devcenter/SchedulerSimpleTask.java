package com.heroku.devcenter;

import org.springframework.amqp.core.AmqpTemplate;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;

/**
 * Simple test task
 */
public class SchedulerSimpleTask {

    public static void main(String[] args) {
        System.out.println("Starting message!");

        SeriesDenormUpdater seriesDenormUpdater = new SeriesDenormUpdater();

        AnnotationConfigApplicationContext context = new AnnotationConfigApplicationContext(RabbitConfiguration.class);
        AmqpTemplate amqpTemplate = context.getBean(AmqpTemplate.class);
        amqpTemplate.convertAndSend(seriesDenormUpdater);

        System.out.println("Sent to RabbitMQ: " + seriesDenormUpdater);

        System.exit(0);
    }
}
