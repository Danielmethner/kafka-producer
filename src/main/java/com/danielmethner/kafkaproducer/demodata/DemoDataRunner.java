package com.danielmethner.kafkaproducer.demodata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.CommandLineRunner;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.Random;

@Component
public class DemoDataRunner  implements CommandLineRunner {

    private static final Logger log = LoggerFactory.getLogger(DemoDataRunner.class);
    private final Random random = new Random();

    @Override
    public void run(String... args) throws Exception {
        log.info("Application Startup completed");
    }

    @Scheduled(fixedRate = 1000)
    public void generateRandomNumber() {
        double randomNumber = (random.nextDouble() * 0.6) + 0.7;
        randomNumber = Math.round(randomNumber * 10000.0) / 10000.0;
        log.info("Generated random number: {}", randomNumber);
    }
}
