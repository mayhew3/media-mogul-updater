package com.heroku.devcenter;

import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitAdmin;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.net.URI;
import java.net.URISyntaxException;

import static java.lang.System.getenv;

@Configuration
public class RabbitConfiguration {

    protected final String helloWorldQueueName = "hello.world.queue";

    @Bean
    public ConnectionFactory connectionFactory() {
        final URI ampqUrl;
        String localamqp_url = getenv("LOCALAMQP_URL");
        String cloudamqp_url = getenv("CLOUDAMQP_URL");

        if (localamqp_url == null && cloudamqp_url == null) {
            throw new IllegalStateException("Need either LOCALAMQP_URL or CLOUDAMQP_URL");
        }

        String amqp_url = localamqp_url == null ? cloudamqp_url : localamqp_url;

        try {
            ampqUrl = new URI(amqp_url);
        } catch (URISyntaxException e) {
            throw new RuntimeException(e);
        }

        final CachingConnectionFactory factory = new CachingConnectionFactory();
        if (localamqp_url == null) {
            factory.setUsername(ampqUrl.getUserInfo().split(":")[0]);
            factory.setPassword(ampqUrl.getUserInfo().split(":")[1]);
            factory.setHost(ampqUrl.getHost());
            factory.setPort(ampqUrl.getPort());
            factory.setVirtualHost(ampqUrl.getPath().substring(1));
        } else {
            factory.setHost("localhost");
        }

        return factory;
    }

    @Bean
    public AmqpAdmin amqpAdmin() {
        return new RabbitAdmin(connectionFactory());
    }

    @Bean
    public RabbitTemplate rabbitTemplate() {
        RabbitTemplate template = new RabbitTemplate(connectionFactory());
        template.setRoutingKey(this.helloWorldQueueName);
        template.setQueue(this.helloWorldQueueName);
        return template;
    }

    @Bean
    public Queue queue() {
        return new Queue(this.helloWorldQueueName);
    }

    private static String getEnvOrThrow(String name) {
        final String env = getenv(name);
        if (env == null) {
            throw new IllegalStateException("Environment variable [" + name + "] is not set.");
        }
        return env;
    }

}
