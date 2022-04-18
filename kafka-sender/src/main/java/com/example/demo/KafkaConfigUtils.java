package com.example.demo;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

public abstract class KafkaConfigUtils {
	
	static String saslJaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{0}\" password=\"{1}\";";
	
	protected static <T> Map<String, Object> producerConfig(KafkaConfig kafkaConfig, String clientId, T t) {
		Map<String, Object> configProps = new HashMap<>();
		configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.kafkaServerPort);
		configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
		configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, t);
		configProps.put(ProducerConfig.ACKS_CONFIG, kafkaConfig.kafkaProducerAcks);
		configProps.put(ProducerConfig.CLIENT_ID_CONFIG, clientId);
		configProps.put(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, Integer.parseInt(kafkaConfig.kafkaProducerMaxInFlightReqPerConn));
		configProps.put(ProducerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, Integer.parseInt(kafkaConfig.kafkaProducerConnMaxIdleMs));
		configProps.put(ProducerConfig.BATCH_SIZE_CONFIG, Integer.parseInt(kafkaConfig.kafkaProducerBatchSize));
		configProps.put(ProducerConfig.LINGER_MS_CONFIG, Integer.parseInt(kafkaConfig.kafkaProducerLingerMs));
		configProps.put(ProducerConfig.BUFFER_MEMORY_CONFIG, Integer.parseInt(kafkaConfig.kafkaProducerBufferMemory));
		configProps.put(ProducerConfig.SEND_BUFFER_CONFIG, Integer.parseInt(kafkaConfig.kafkaProducerSendBufferBytes));
		configProps.put(ProducerConfig.RECEIVE_BUFFER_CONFIG, Integer.parseInt(kafkaConfig.kafkaProducerReceiveBufferBytes));
		configProps.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, Integer.parseInt(kafkaConfig.kafkaProducerMaxBlockMs));
		if (StringUtils.isNotBlank(kafkaConfig.kafkaProducerEnableIdempotenceFlag) && "Y".equals(kafkaConfig.kafkaProducerEnableIdempotenceFlag) && "all".equals(kafkaConfig.kafkaProducerAcks)) {
			configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
		} else {
			configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, false);
		}
		if (StringUtils.isNotBlank(kafkaConfig.kafkaProducerCompressionTypeFlag) && "Y".equals(kafkaConfig.kafkaProducerCompressionTypeFlag)) {
			configProps.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "lz4");
		}
		
		if (StringUtils.isNotBlank(kafkaConfig.kafkaUsername) && StringUtils.isNotBlank(kafkaConfig.kafkaPassword)) {
			configProps.put("security.protocol", "SASL_SSL");
			configProps.put("sasl.mechanism", "PLAIN");
			configProps.put("sasl.jaas.config", MessageFormat.format(saslJaasConfig, kafkaConfig.kafkaUsername, kafkaConfig.kafkaPassword));
			configProps.put("ssl.truststore.location", kafkaConfig.kafkaTrustStoreLocation);
			configProps.put("ssl.truststore.password", kafkaConfig.kafkaTrustStorePassword);
		}
		
		return configProps;
	}
	
	protected static ProducerFactory<String, String> createProducerFactory(KafkaConfig kafkaConfig, String clientId) throws Exception {
		return new DefaultKafkaProducerFactory<>(producerConfig(kafkaConfig, clientId, StringSerializer.class));
	}
	protected static ProducerFactory<String, String> createProducerFactory(Map<String, Object> producerConfig) throws Exception {
		return new DefaultKafkaProducerFactory<>(producerConfig);
	}
	
	protected static KafkaTemplate<String, String> createKafkaTemplate(ProducerFactory<String, String> producerFactory) throws Exception {
		return new KafkaTemplate<>(producerFactory);
	}
}
