package com.trident.retail_analysis;

import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;
import org.apache.storm.kafka.trident.TridentKafkaConfig;
import org.apache.storm.spout.SchemeAsMultiScheme;

public class SpoutBuilder {

    public static TransactionalTridentKafkaSpout buildKafkaSpout(BrokerHosts hosts, String topic, String clientId) {
        TridentKafkaConfig kafkaConfig = new TridentKafkaConfig(hosts, topic, clientId);
        kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
        return new TransactionalTridentKafkaSpout(kafkaConfig);
    }
}
