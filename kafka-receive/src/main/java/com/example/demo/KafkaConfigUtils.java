package com.example.demo;

import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

public abstract class KafkaConfigUtils {
	
	static String saslJaasConfig = "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{0}\" password=\"{1}\";";
	
	protected static Map<String, Object> consumerConfig(KafkaConfig kafkaConfig, String groupId, String clientId, Integer maxPollRecords, boolean allowAutoCreateTopicFlag, boolean enableAutoCommit) {
		Map<String, Object> config = new HashMap<>();
		config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaConfig.kafkaServerPort);
		config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
		config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, enableAutoCommit);
		config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, kafkaConfig.kafkaConsumerAutoOffsetReset);
		config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
		config.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, maxPollRecords);
		config.put(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, allowAutoCreateTopicFlag);
		config.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
		config.put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, Integer.parseInt(kafkaConfig.kafkaConsumerReceiveBufferBytes));
		config.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, Integer.parseInt(kafkaConfig.kafkaConsumerFetchMaxWaitMs));
		config.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, Integer.parseInt(kafkaConfig.kafkaConsumerFetchMinBytes));
		config.put(ConsumerConfig.CONNECTIONS_MAX_IDLE_MS_CONFIG, Integer.parseInt(kafkaConfig.kafkaConsumerConnMaxIdleMs));
		config.put(ConsumerConfig.SEND_BUFFER_CONFIG, Integer.parseInt(kafkaConfig.kafkaConsumerSendBufferBytes));
		config.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, Integer.parseInt(kafkaConfig.kafkaConsumerPollMs));
		config.put(ConsumerConfig.FETCH_MAX_BYTES_CONFIG, Integer.parseInt(kafkaConfig.kafkaConsumerFetchMaxBytes));
		config.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, Integer.parseInt(kafkaConfig.kafkaConsumerMaxPartitionFetchBytes));
		config.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, Integer.parseInt(kafkaConfig.kafkaConsumerHeartbeatIntervalMs));
		config.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, Integer.parseInt(kafkaConfig.kafkaConsumerSessionTimeoutMs));
		
		if (StringUtils.isNotBlank(kafkaConfig.kafkaUsername) && StringUtils.isNotBlank(kafkaConfig.kafkaPassword)) {
			config.put("security.protocol", "SASL_SSL");
			config.put("sasl.mechanism", "PLAIN");
			config.put("sasl.jaas.config", MessageFormat.format(saslJaasConfig, kafkaConfig.kafkaUsername, kafkaConfig.kafkaPassword));
			config.put("ssl.truststore.location", kafkaConfig.kafkaTrustStoreLocation);
			config.put("ssl.truststore.password", kafkaConfig.kafkaTrustStorePassword);
		}
		
		return config;
	}
	
	// consumer kafka
	protected static KafkaConsumer<String, String> kafKaConsumer(KafkaConfig kafkaConfig, String groupId, String clientId, Integer maxPollRecords, boolean allowAutoCreateTopicFlag, boolean enableAutoCommit) throws Exception {
		return new KafkaConsumer<>(consumerConfig(kafkaConfig, groupId, clientId, maxPollRecords, allowAutoCreateTopicFlag, enableAutoCommit));
	}
	protected static KafkaConsumer<String, String> kafKaConsumer(Map<String, Object> config) throws Exception {
		return new KafkaConsumer<>(config);
	}
	
	// consumer factory for consumer reply kafka
	protected static ConsumerFactory<String, String> consumerFactory(KafkaConfig kafkaConfig, String groupId, String clientId, Integer maxPollRecords, boolean allowAutoCreateTopicFlag, boolean enableAutoCommit) throws Exception {
		return new DefaultKafkaConsumerFactory<>(consumerConfig(kafkaConfig, groupId, clientId, maxPollRecords, allowAutoCreateTopicFlag, enableAutoCommit));
	}
	protected ConsumerFactory<String, String> consumerFactory(Map<String, Object> config) throws Exception {
		return new DefaultKafkaConsumerFactory<>(config);
	}
	
	// Concurrent Kafka Listener Container Factory
	protected static ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory,  Integer concurrency) throws Exception {
	    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
	    factory.setConsumerFactory(consumerFactory);
	    factory.setConcurrency(1);
	    if (null != concurrency) {
	    	factory.setConcurrency(concurrency);
	    }
	    return factory;
	}
	protected static ConcurrentKafkaListenerContainerFactory<String, String> kafkaListenerContainerFactory(ConsumerFactory<String, String> consumerFactory, KafkaTemplate<String, String> kafkaTemplate) throws Exception {
	    ConcurrentKafkaListenerContainerFactory<String, String> factory = new ConcurrentKafkaListenerContainerFactory<>();
	    factory.setConsumerFactory(consumerFactory);
	    factory.setReplyTemplate(kafkaTemplate);
	    factory.setConcurrency(1);
	    factory.setAutoStartup(true);
	    return factory;
	}
	
	// Concurrent Message Listener Container
	protected static ConcurrentMessageListenerContainer<String, String> concurrentMessageListenerContainer(ConcurrentKafkaListenerContainerFactory<String, String> factory, String replyTopic, String groupIdReplyTopic) throws Exception {
		ConcurrentMessageListenerContainer<String, String> replyContainer = factory.createContainer(replyTopic);
		replyContainer.getContainerProperties().setMissingTopicsFatal(true);
		replyContainer.getContainerProperties().setGroupId(groupIdReplyTopic);
		replyContainer.setAutoStartup(true);
		return replyContainer;
	}
}
