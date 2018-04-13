package com.trident.retail_analysis;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;

import java.util.UUID;

public class SpoutBuilder {

    public static TransactionalTridentKafkaSpout buildKafkaSpout(BrokerHosts hosts, String topic) {
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(hosts, topic, UUID.randomUUID().toString());
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        kafkaConfig.forceFromStart = true;

        return new TransactionalTridentKafkaSpout(kafkaConfig);
    }
}
