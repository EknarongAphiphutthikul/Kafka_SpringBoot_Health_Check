package com.example.demo;

import java.util.Properties;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.AbstractHealthIndicator;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.Status;
import org.springframework.stereotype.Component;

@Component("custom-kafka")
public class CustomKafkaHealthIndicator extends AbstractHealthIndicator {
	
	@Autowired
	private KafkaConfig kafkaConfig;

	@Override
	protected void doHealthCheck(Health.Builder builder) {
		 Properties properties = new Properties();
		    properties.put("bootstrap.servers", kafkaConfig.kafkaServerPort);
		    properties.put("connections.max.idle.ms", 10000);
		    properties.put("request.timeout.ms", 5000);
		try (AdminClient client = AdminClient.create(properties)) {
			ListTopicsResult resultTopic = client.listTopics();
			try {
				for (String topicName : resultTopic.names().get()) System.out.println(topicName); //TODO implement check topic
				builder.status(Status.UP);
			} catch (Exception e) {
				e.printStackTrace();
				builder.status(Status.DOWN);
			}
		}
	}
}
