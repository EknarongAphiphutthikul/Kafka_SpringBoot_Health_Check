package com.example.demo;

import java.util.Map;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Headers;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class Controller {

	@Autowired
	@Qualifier("kafkaTemplateSender")
	private KafkaTemplate<String, String> kafkaTemplate;
	
	@PostMapping("send")
	public @ResponseBody String kafkaSender(@RequestBody ModelReq request) throws Exception {
		if (null != request.getHeaders() && !request.getHeaders().isEmpty()) {
			Headers header = new RecordHeaders();
			for (Map.Entry<String, String> key : request.getHeaders().entrySet()) {
				header.add(new RecordHeader(key.getKey(), key.getValue().getBytes()));
			}
			ProducerRecord<String, String> record = new ProducerRecord<>(request.getTopic(), null, System.currentTimeMillis(), null, request.getMsg(), header);
			kafkaTemplate.send(record);
		} else {
			kafkaTemplate.send(request.getTopic(), request.getMsg());
		}
		return "OK";
	}
}
