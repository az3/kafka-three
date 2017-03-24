package com.cagricelebi.kafka.three.metric;

import io.prometheus.client.Gauge;
import io.prometheus.client.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author cagricelebi
 */
public class PrometheusHelper {

    private static final Logger logger = LoggerFactory.getLogger(PrometheusHelper.class);

    private final String partitionId;

    private static final Gauge prometheusApproxArrivalTimeGauge = Gauge.build()
            .name("kafka_consumer_approximate_arrival_time_diff_seconds")
            .help("Approximate arrival time is a metadata inside a Kafka record, this shows the difference between record.timestamp and consume time in seconds.")
            .labelNames("shard_id").register();

    private static final Histogram prometheusHistogram = Histogram.build()
            .buckets(400d, 1000d, 10000d)
            .name("kinesis_consumer_records")
            .help("Tracks the data passing though a partition, shows record size in bytes.")
            .labelNames("partition_id").register();

    public PrometheusHelper(String partitionId) {
        this.partitionId = partitionId;
    }

    public void calculateSize(double recSize) {
        prometheusHistogram.labels(partitionId).observe(recSize);
    }

    public void calculateApproxArrivalTimeDiff(long approximateArrivalTimestamp) {
        long diff = (System.currentTimeMillis() - approximateArrivalTimestamp) / 1000L;
        prometheusApproxArrivalTimeGauge.labels(partitionId).set(diff);
    }

    public void shutdown() {
        prometheusHistogram.clear();
        prometheusApproxArrivalTimeGauge.clear();
    }
}
