package com.transactis.kafkaintegration.consumer;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class MessageConsumer {
	
	@KafkaListener(topics = "mytopicorsys", groupId = "my-group-id" )
	public void listen(String message) {
		System.out.println("Receveid message : " +message);
	}

}
