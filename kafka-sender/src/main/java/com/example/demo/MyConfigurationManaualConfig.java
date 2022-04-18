package com.example.demo;

import java.util.UUID;

import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

@Configuration
public class MyConfigurationManaualConfig extends KafkaConfigUtils {
	
	@Autowired
	private KafkaConfig kafkaConfig;
	
	@Bean("initProducerFactory")
	public ProducerFactory<String, String> initProducerFactory() throws Exception {
		getKafkaServerPort();
		return createProducerFactory(kafkaConfig, UUID.randomUUID().toString());
	}

	@Bean("kafkaTemplateSender")
	public KafkaTemplate<String, String> initKafkaTemplate(@Qualifier("initProducerFactory") ProducerFactory<String, String> producerFactory) throws Exception {
		return createKafkaTemplate(producerFactory);
	}
	
	private String getKafkaServerPort() throws Exception {
		String serverPort = kafkaConfig.kafkaServerPort;
		if (StringUtils.isBlank(serverPort)) {
			throw new RuntimeException("KAFKA_SERVER_PORT is blank.");
		}
		return serverPort;
	}
}
