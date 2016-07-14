package com.cj.messagestreaming;

import kafka.consumer.ConsumerConfig;

public class KafkaConfig {
	
	private final ConsumerConfig consumerConfig;

	public KafkaConfig(ConsumerConfig consumerConfig){
		this.consumerConfig = consumerConfig;
	}

	public ConsumerConfig getConsumerConfig() {
		return consumerConfig;
	}

}
