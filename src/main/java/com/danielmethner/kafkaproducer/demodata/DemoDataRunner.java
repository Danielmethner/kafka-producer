package com.danielmethner.kafkaproducer.demodata;

import jakarta.annotation.PreDestroy;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Properties;
import java.util.Random;

@Component
public class DemoDataRunner implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoDataRunner.class);
    private final Random random = new Random();

    private KafkaProducer<String, String> kafkaProducer;

    @Scheduled(fixedRate = 500)
    public void generateRandomNumber() {
        double randomNumber = (random.nextDouble() * 0.6) + 0.7;
        randomNumber = Math.round(randomNumber * 10000.0) / 10000.0;
        if (kafkaProducer != null) {
            ProducerRecord<String, String> kafkaRecord = new ProducerRecord<>("market-data", "random-number", String.valueOf(randomNumber));
            log.info("Sending random number: {}", randomNumber);
            kafkaProducer.send(kafkaRecord);
            kafkaProducer.flush();
        }
        log.info("Generated random number: {}", randomNumber);
    }

    @Override
    public void run(String... args) throws Exception {
        log.info("Starting DemoDataRunner");
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        this.kafkaProducer = new KafkaProducer<>(properties);
    }

    @PreDestroy
    public void close() {
        log.info("Closing DemoDataRunner");
        if (kafkaProducer != null) {
            kafkaProducer.close();
            log.info("Closed KafkaProducer");
        }
    }
}
