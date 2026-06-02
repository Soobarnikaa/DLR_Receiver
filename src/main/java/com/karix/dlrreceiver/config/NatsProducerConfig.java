package com.karix.dlrreceiver.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import com.karix.nats.core.producer.ProducerManager;
import com.karix.nats.core.service.GenericInitializer;
import org.springframework.context.annotation.DependsOn;

@Configuration
public class NatsProducerConfig {

	@Value("${karix.nats.component}")
	private String component;

	@Bean
	@DependsOn("natsInitializer")
	public ProducerManager producerManager() {
		return new ProducerManager(GenericInitializer.getClusterManager(), component);
	}
}