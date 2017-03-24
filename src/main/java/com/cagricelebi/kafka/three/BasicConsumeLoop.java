package com.cagricelebi.kafka.three;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample.
 * 1. http://docs.confluent.io/3.2.0/clients/consumer.html#detailed-examples
 * 2. https://github.com/apache/kafka/blob/trunk/examples/src/main/java/kafka/examples/Consumer.java
 *
 * @author cagricelebi
 */
public abstract class BasicConsumeLoop implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(BasicConsumeLoop.class);

    private Properties config;
    private KafkaConsumer<String, String> consumer;
    private final long pollInterval;
    private final List<String> topics;
    private final AtomicBoolean shutdown;
    private final CountDownLatch shutdownLatch;

    public BasicConsumeLoop(Properties config, List<String> topics, long pollInterval) {
        this.config = config;
        this.topics = topics;
        this.pollInterval = pollInterval;
        this.shutdown = new AtomicBoolean(false);
        this.shutdownLatch = new CountDownLatch(1);
    }

    @Override
    public void run() {
        try {
            logger.info("Starting KafkaConsumer initialization.");
            consumer = new KafkaConsumer<>(config);
            logger.info("After KafkaConsumer initialization.");
            logger.info("Before consumer.subscribe.");
            consumer.subscribe(topics);
            logger.info("After consumer.subscribe.");
            logger.info("Starting while loop.");
            while (!shutdown.get()) {
                // logger.info("Before polling.");
                ConsumerRecords<String, String> records = consumer.poll(pollInterval);
                // logger.info("After polling.");
                process(records);
                consumer.commitAsync();
            }
            logger.info("End of while loop.");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            try {
                consumer.commitSync();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            logger.info("Before consumer close.");
            try {
                consumer.close();
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }
            logger.info("After consumer close.");
            shutdownLatch.countDown();
        }

    }

    public abstract void process(ConsumerRecords<String, String> record);

    public void shutdown() throws InterruptedException {
        logger.info("Started shutdown sequence.");
        shutdown.set(true);
        shutdownLatch.await();
        logger.info("Completed shutdown sequence.");
    }
}
