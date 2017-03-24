package com.cagricelebi.kafka.three;

import com.cagricelebi.kafka.three.metric.PrometheusRunnable;
import java.io.FileInputStream;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cagricelebi
 */
public class Starter {

    private static final Logger logger = LoggerFactory.getLogger(Starter.class);

    private ExecutorService prometheusExecutor;
    int prometheusPort;
    String prometheusShutdownKey;

    private ExecutorService kafkaExecutor;
    private BasicConsumeLoop worker;

    private AtomicBoolean shutdown;
    private CountDownLatch shutdownLatch;

    public static void main(String[] args) {
        try {
            Starter s = new Starter();
            s.start();
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private void start() {
        try {

            shutdown = new AtomicBoolean(false);
            shutdownLatch = new CountDownLatch(1);
            registerHooks();
            Properties properties = getConfig();

            prometheusExecutor = Executors.newSingleThreadExecutor();
            prometheusShutdownKey = new BigInteger(130, new SecureRandom()).toString(32);
            prometheusPort = Integer.parseInt(properties.getProperty("prometheusPort", "8090"));
            logger.info("Prometheus on port {} with shutdownKey: '{}'", prometheusPort, prometheusShutdownKey);
            prometheusExecutor.submit(new PrometheusRunnable(prometheusShutdownKey, prometheusPort));

            long pollInterval = Long.parseLong(properties.getProperty("pollInterval", "1000"));
            String kafkaInputTopic = properties.getProperty("kafkaInputTopic");
            logger.info("started threading consumer.");
            threading(properties, kafkaInputTopic, pollInterval);
            logger.info("completed threading consumer.");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private Properties getConfig() {

        String configFile = System.getProperty("CONFIG_FILE");
        if (configFile == null || configFile.isEmpty()) {
            String msg = "Could not load properties file -DCONFIG_FILE from classpath";
            throw new IllegalStateException(msg);
        }

        Properties properties = new Properties();

        try (InputStream configStream = new FileInputStream(configFile)) {
            properties.load(configStream);
        } catch (Exception e) {
            String msg = "Could not load properties file -DCONFIG_FILE from classpath";
            throw new IllegalStateException(msg, e);
        }
        return properties;
    }

    private void threading(Properties properties, String kafkaInputTopic, long pollInterval) {
        List<String> topics = new ArrayList<>();
        topics.add(kafkaInputTopic);
        logger.info("Starting Kafka Executors.");
        kafkaExecutor = Executors.newFixedThreadPool(10);
        worker = new SampleRecordProcessor(properties, topics, pollInterval);
        kafkaExecutor.submit(worker);
        logger.info("Completed initialization of Kafka Executors.");
    }

    // <editor-fold defaultstate="collapsed">
    private void freezing(Properties properties, String kafkaInputTopic, long idleTimeBetweenReadsInMillis) {
        List<String> topics = new ArrayList<>();
        topics.add(kafkaInputTopic);

        KafkaConsumer consumer = null;
        try {
            logger.info("Starting KafkaConsumer initialization.");
            consumer = new KafkaConsumer<>(properties);
            logger.info("After KafkaConsumer initialization.");
            logger.info("Before consumer.subscribe to topic {}.", kafkaInputTopic);
            consumer.subscribe(topics);
            logger.info("After consumer.subscribe to topic {}.", kafkaInputTopic);
            logger.info("Starting while loop.");
            while (!shutdown.get()) {
                logger.info("Before polling.");
                ConsumerRecords<String, String> records = consumer.poll(idleTimeBetweenReadsInMillis);
                logger.info("After polling.");
                for (ConsumerRecord<String, String> record : records) {
                    String recordString = record.value();
                    logger.info("Record.Value: '{}'.", recordString);
                }
            }
            logger.info("End of while loop.");
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        } finally {
            logger.info("Before consumer close 1.");
            if (consumer != null) {
                logger.info("Before consumer close 2.");
                consumer.close();
            }
            logger.info("After consumer close.");
            shutdownLatch.countDown();
        }

    }

    private void registerHooks() {
        logger.info("Registering INT/TERM signal for shutdown.");
        SignalRegister.attach(new String[]{"INT", "TERM"}, new SignalRegister.Listener() {
            @Override
            public synchronized void handleSignal() {
                logger.warn("INT/TERM signal cought by Starter.");
                try {
                    String url = "http://localhost:" + prometheusPort + "/shutdown?key=" + prometheusShutdownKey;
                    get(url);
                    prometheusExecutor.shutdown();
                    kafkaExecutor.shutdown();

                    shutdown.set(true);
                    if (worker != null) {
                        worker.shutdown();
                    }
                    // shutdownLatch.await();
                } catch (Exception e) {
                    e.printStackTrace();
                }
                logger.warn("Shutdown complete.");
                System.exit(0);
            }
        });
    }

    private int get(String endPoint) throws Exception {
        HttpURLConnection httpCon = (HttpURLConnection) (new URL(endPoint)).openConnection();
        // httpCon.setRequestMethod("GET");
        // httpCon.getInputStream();
        return httpCon.getResponseCode();
    }
    // </editor-fold>

}
