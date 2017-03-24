#/usr/bin/env bash
while true; do
    echo `date` >>prometheus.log
    curl -s localhost:9751/metrics | grep kafka_consumer_approximate_arrival_time_diff_seconds >>prometheus.log
    sleep 60
done
