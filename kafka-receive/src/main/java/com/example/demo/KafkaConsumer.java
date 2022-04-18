package com.example.demo;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class KafkaConsumer extends KafkaConfigUtils {
	
	private Long consumerPoll;
	private int consumerMaxPollRecords;
	private String topic;
	private String groupId;
	private ExecutorService executor;
	private final CountDownLatch latch;
	private ThreadPoolExecutor executorWorker;
	private org.apache.kafka.clients.consumer.KafkaConsumer<String, String> consumer;
	
	private boolean booleanKafkaCommitSync = false;
	@Autowired
	private KafkaConfig kafkaConfig;
	
	public KafkaConsumer() {
		latch = new CountDownLatch(1);
	}
	
	public ThreadPoolExecutor getExecutorServiceWorker() {
		return this.executorWorker;
	}

	@PostConstruct
	public void start() throws Exception {
		prepareConfig();
		startListening();
	}
	
	private void prepareConfig() throws Exception {
		consumerPoll = Long.parseLong(StringUtils.defaultIfEmpty(kafkaConfig.kafkaConsumerPollMs, "1800"));
		consumerMaxPollRecords = Integer.parseInt(StringUtils.defaultIfEmpty(kafkaConfig.kafkaConsumerMaxPollRecords, "20"));
		topic = kafkaConfig.kafkaTopic;
		groupId = "kafka-receive";
		executor = Executors.newSingleThreadExecutor();
		executorWorker = (ThreadPoolExecutor) Executors.newFixedThreadPool(Integer.parseInt(StringUtils.defaultIfEmpty(kafkaConfig.kafkaConsumerThread, "5")));
		executorWorker.prestartAllCoreThreads();
		consumer = kafKaConsumer(kafkaConfig, groupId, UUID.randomUUID().toString(), consumerMaxPollRecords, false, false);
	}
	
	private void startListening() {
		executor.submit(getListenTask());
	}
	
	private Runnable getListenTask() {
		return () -> {
			consumer.subscribe(Collections.singletonList(topic));
			try {
				while (true) {
				    try {
				        pollRecords();
				    }catch (InterruptedException ex) {
				        ex.printStackTrace();
				    } catch (ExecutionException ex) {
				       ex.printStackTrace();
				    }
				}
			} catch (WakeupException ex) {
				ex.printStackTrace();
			} catch (Exception ex) {
				ex.printStackTrace();
			} finally {
				if (!this.booleanKafkaCommitSync) {
					try {
						consumer.commitSync();
					} catch (Exception e) {
						e.printStackTrace();
					}
				}
				consumer.close();
				latch.countDown();
			}
		};
	}
	
	private void pollRecords() throws InterruptedException, ExecutionException {
		ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(consumerPoll));
		if (records.count() != 0) {
			if (this.booleanKafkaCommitSync) {
				consumer.commitSync();
			} else {
				consumer.commitAsync();
			}
			Iterator<ConsumerRecord<String, String>> iterator = records.iterator();
			List<Callable<Void>> task = new ArrayList<>();
			while (iterator.hasNext()) {
				ConsumerRecord<String, String> record = iterator.next();
				task.add(new Callable<Void>() {
					public Void call() throws Exception {
						process(record);
				        return null;
				    }
				});
			}
			
			executorWorker.invokeAll(task);
		}			
	}
	
	private void process(ConsumerRecord<String, String> record) {
		System.out.println(record.value());
	}
	
	@PreDestroy
	private void addShutDownHook() {
		stopConsumer();
		closeExecutor();
	}

	public void stopConsumer() {
		consumer.wakeup();
		try {
			latch.await();
		} catch (InterruptedException ex) {
			ex.printStackTrace();
		}
	}

	public void closeExecutor() {
		executorWorker.shutdownNow();
		executor.shutdown();
		try {
			if (!executor.awaitTermination(5, TimeUnit.SECONDS)) {
				executor.shutdownNow();
			}
		} catch (InterruptedException ex) {
			ex.printStackTrace();
			executor.shutdownNow();
			Thread.currentThread().interrupt();
		}
	}
}
