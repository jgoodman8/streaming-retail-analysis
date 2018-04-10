package com.trident.retail_analysis;

import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.FirstN;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.operation.builtin.TupleCollectionGet;
import storm.trident.testing.MemoryMapState;

public class Topology {

    private static final int N = 5;

    private static final String COUNTRY = "Country";
    private static final String QUANTITY = "Quantity";
    private static final String UNIT_PRICE = "UnitPrice";
    private static final String TOTAL_PRICE = "total_price";
    private static final String TOTAL_VOLUME = "total_volume";

    private static final String[] REQUIRED_FIELDS = {QUANTITY, UNIT_PRICE, COUNTRY};
    private static final String[] CSV_FIELDS = {"InvoiceNo", "StockCode", "Description", QUANTITY,
            "InvoiceDate", UNIT_PRICE, "CustomerID", COUNTRY};

    public static StormTopology buildTopology(TransactionalTridentKafkaSpout kafkaSpout, LocalDRPC localDRPC) {
        TridentTopology topology = new TridentTopology();

        TridentState totalVolume = topology
                .newStream("purchases_stream", kafkaSpout)
                .each(new Fields("str"), new SplitCSV(), new Fields(CSV_FIELDS))
                .project(new Fields(REQUIRED_FIELDS))
                .each(new Fields(QUANTITY, UNIT_PRICE), new TotalPrice(), new Fields(TOTAL_PRICE))
                .groupBy(new Fields(COUNTRY))
                .persistentAggregate(new MemoryMapState.Factory(), new Sum(), new Fields(TOTAL_VOLUME));

        topology.newDRPCStream(RetailAnalysis.TOP_SOLD, localDRPC)
                .stateQuery(totalVolume, new TupleCollectionGet(), new Fields(COUNTRY))
                .stateQuery(totalVolume, new MapGet(), new Fields(TOTAL_VOLUME))
                .groupBy(new Fields(COUNTRY))
                .aggregate(new Fields(COUNTRY, TOTAL_VOLUME),
                        new FirstN.FirstNSortedAgg(N, TOTAL_VOLUME, true),
                        new Fields(COUNTRY, TOTAL_VOLUME));

        topology.newDRPCStream(RetailAnalysis.TOP_CANCELED, localDRPC)
                .stateQuery(totalVolume, new TupleCollectionGet(), new Fields(COUNTRY))
                .stateQuery(totalVolume, new Fields(COUNTRY), new MapGet(), new Fields(TOTAL_VOLUME))
                .groupBy(new Fields(COUNTRY))
                .aggregate(new Fields(COUNTRY, TOTAL_VOLUME),
                        new FirstN.FirstNSortedAgg(N, TOTAL_VOLUME, false),
                        new Fields(COUNTRY, TOTAL_VOLUME));

        return topology.build();
    }
}