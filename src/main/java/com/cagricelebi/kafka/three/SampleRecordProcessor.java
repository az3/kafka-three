package com.cagricelebi.kafka.three;

import com.cagricelebi.kafka.three.metric.PrometheusHelper;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Sample.
 * 1. http://docs.confluent.io/3.2.0/clients/consumer.html#detailed-examples
 * 2. https://github.com/apache/kafka/blob/trunk/examples/src/main/java/kafka/examples/Consumer.java
 *
 * @author cagricelebi
 */
public class SampleRecordProcessor extends BasicConsumeLoop {

    private static final Logger logger = LoggerFactory.getLogger(SampleRecordProcessor.class);

    private PrometheusHelper prometheus;

    public SampleRecordProcessor(Properties config, List<String> topics, long idleTimeBetweenReadsInMillis) {
        super(config, topics, idleTimeBetweenReadsInMillis);
    }

    @Override
    public void process(ConsumerRecords<String, String> records) {
        try {
            int numRec = records.count();
            long start = System.currentTimeMillis();

            Set<TopicPartition> topicPartitions = records.partitions();
            List<Integer> partitions = new ArrayList<>();
            for (TopicPartition elem : topicPartitions) {
                partitions.add(elem.partition());
                logger.info("topic/partition: {}/{}.", elem.topic(), elem.partition());
            }

            for (ConsumerRecord<String, String> record : records) {
                String recordString = record.value();
                // logger.info("Record.Value: '{}'.", recordString);
                logger.info("Record.Object: '{}'.", dumpKafkaRecord(record));
            }
            if (numRec > 0) {
                long scripttimerEmitComplete = System.currentTimeMillis() - start;
                logger.info("processRecords completed in {} ms for {} records.", scripttimerEmitComplete, numRec);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private String dumpKafkaRecord(ConsumerRecord<String, String> record) {
        JSONObject obj = new JSONObject();
        //obj.put("topic", record.topic());
        obj.put("checksum", record.checksum());
        obj.put("offset", record.offset());
        obj.put("partition", record.partition());
        //obj.put("timestampType", record.timestampType());
        //obj.put("timestamp", record.timestamp());

        //obj.put("key", record.key());
        //obj.put("serializedKeySize", record.serializedKeySize());
        obj.put("value", record.value());
        obj.put("serializedValueSize", record.serializedValueSize());
        return obj.toString();
    }

    private void prometheus(byte[] bytea, long approxTime) {
        try {
            prometheus.calculateSize(bytea.length);
            if (approxTime > -1) {
                prometheus.calculateApproxArrivalTimeDiff(approxTime);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

}
