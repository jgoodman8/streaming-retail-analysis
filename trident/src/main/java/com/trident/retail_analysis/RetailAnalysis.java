package com.trident.retail_analysis;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.LocalDRPC;
import org.apache.storm.kafka.BrokerHosts;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.TransactionalTridentKafkaSpout;

public class RetailAnalysis {

    private static final String BROKER_ZOOKEEPER = "172.17.0.5:2181";
    private static final String TOPOLOGY_NAME = "trident-retail-analysis";

    private static final String CLIENT_ID = "storm";
    private static final String INPUT_TOPIC = "compras-trident";

    public static final String TOP_SOLD = "top_sold_purchases";
    public static final String TOP_CANCELED = "top_canceled_purchases";

    public static void main(String[] args) throws Exception {

        LocalDRPC localDRPC = new LocalDRPC();
        Config configuration = new Config();
        configuration.setMaxSpoutPending(20);

        BrokerHosts hosts = new ZkHosts(BROKER_ZOOKEEPER);
        TransactionalTridentKafkaSpout kafkaSpout = SpoutBuilder.buildKafkaSpout(hosts, INPUT_TOPIC, CLIENT_ID);

        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology(TOPOLOGY_NAME, configuration, Topology.buildTopology(kafkaSpout));

        for (int i = 0; i < 100; i++) {
            System.out.println("Sum: " + localDRPC.execute(TOP_SOLD, "good happy"));
            System.out.println("Sum: " + localDRPC.execute(TOP_CANCELED, "good happy"));
            Thread.sleep(1000);
        }

        localDRPC.shutdown();
        cluster.shutdown();
    }
}
