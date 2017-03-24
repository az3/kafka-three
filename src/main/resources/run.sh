#/usr/bin/env bash
nohup java -server -XX:+UseG1GC -DCONFIG_FILE=/data/kafka/prod.properties -jar /data/kafka/kafka-three.jar >>/data/kafka/out.log 2>&1 &
