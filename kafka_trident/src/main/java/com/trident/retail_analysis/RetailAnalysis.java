package com.trident.retail_analysis;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import storm.kafka.BrokerHosts;
import storm.kafka.ZkHosts;
import storm.kafka.trident.TransactionalTridentKafkaSpout;

public class RetailAnalysis {

    private static final String KAFKA_TOPIC = "compras-trident";
    private static final String BROKER_ZOOKEEPER = "namenode:2181";
    private static final String TOPOLOGY_NAME = "trident-retail-analysis";

    public static final String TOP_SOLD = "top_sold_purchases";
    public static final String TOP_CANCELED = "top_canceled_purchases";

    public static void main(String[] args) throws Exception {

        LocalDRPC localDRPC = new LocalDRPC();
        Config configuration = new Config();
        configuration.setMaxSpoutPending(20);

        BrokerHosts hosts = new ZkHosts(BROKER_ZOOKEEPER);
        TransactionalTridentKafkaSpout kafkaSpout = SpoutBuilder.buildKafkaSpout(hosts, KAFKA_TOPIC);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, configuration, Topology.buildTopology(kafkaSpout, localDRPC));

        for (int i = 0; i < 100; i++) {
            System.out.println("Top 5: " + localDRPC.execute(TOP_SOLD, "France UK"));
//            System.out.println("Sum: " + localDRPC.execute(TOP_CANCELED, "good happy"));
            Thread.sleep(1000);
        }

        localDRPC.shutdown();
        cluster.shutdown();
    }
}
