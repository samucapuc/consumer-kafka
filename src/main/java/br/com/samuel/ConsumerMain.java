package br.com.samuel;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerMain {
	private static Logger logger = LoggerFactory.getLogger(ConsumerMain.class);
	public static void main(String[] args) {
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties());
		consumer.subscribe(Collections.singletonList("TOPICO_SAMUEL"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
			if (!records.isEmpty()) {
				logger.info("Encontrei {} registros",records.count());
				for (ConsumerRecord<String, String> record : records) {
					logger.info("------------------------------------------");
					logger.info("Processando registro {}, {}, {}, {}", record.key(),record.value(),record.partition(),record.offset());
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						logger.error( "Interrupted!", e);
					    Thread.currentThread().interrupt();
					}
					logger.info("Tópico lido com sucesso!");
				}
			}
		}
	}

	private static Properties properties() {
		Properties properties = new Properties();
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "172.19.81.202:9092");
		properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
		properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, "GRUPO1");
		properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
		properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		return properties;
	}
}
