package com.trident.retail_analysis;

import backtype.storm.spout.SchemeAsMultiScheme;
import storm.kafka.BrokerHosts;
import storm.kafka.StringScheme;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.kafka.trident.TridentKafkaConfig;

public class SpoutBuilder {

    public static TransactionalTridentKafkaSpout buildKafkaSpout(BrokerHosts hosts, String topic, String clientId) {
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(hosts, topic, clientId);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new TransactionalTridentKafkaSpout(kafkaConfig);
    }
}
