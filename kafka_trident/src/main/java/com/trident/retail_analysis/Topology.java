package com.trident.retail_analysis;

import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.builtin.*;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;

public class Topology {

    private static final int N = 5;

    private static final String COUNTRY = "Country";
    private static final String QUANTITY = "Quantity";
    private static final String CUSTOMER = "CustomerID";
    private static final String UNIT_PRICE = "UnitPrice";

    private static final String TOTAL_PRICE = "total_price";
//    private static final String TOTAL_VOLUME = "total_volume";

    private static final String TOTAL_SALES = "total_sales";
    private static final String TOTAL_CANCELLATIONS = "total_cancellations";

    private static final String[] REQUIRED_FIELDS = {QUANTITY, UNIT_PRICE, COUNTRY, CUSTOMER};
    private static final String[] CSV_FIELDS = {"InvoiceNo", "StockCode", "Description", QUANTITY,
            "InvoiceDate", UNIT_PRICE, "CustomerID", COUNTRY};

    public static StormTopology buildTopology(TransactionalTridentKafkaSpout kafkaSpout, LocalDRPC localDRPC) {
        TridentTopology topology = new TridentTopology();

        TridentState totalVolume = topology
                .newStream("purchases_stream", kafkaSpout)
                .each(new Fields("str"), new SplitCSV(), new Fields(CSV_FIELDS))
                .project(new Fields(REQUIRED_FIELDS))
                .each(new Fields(QUANTITY, UNIT_PRICE), new TotalPrice(), new Fields(TOTAL_PRICE))
                .groupBy(new Fields(CUSTOMER))
                .aggregate(new Fields(TOTAL_PRICE), new VolumeAggregator(), new Fields(TOTAL_SALES, TOTAL_CANCELLATIONS))
                .persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("count"))
                .parallelismHint(16);

        topology.newDRPCStream(RetailAnalysis.TOP_SOLD, localDRPC)
                .stateQuery(totalVolume, new TupleCollectionGet(), new Fields(COUNTRY, TOTAL_SALES, CUSTOMER))
                .each(new Fields("args"), new Split(), new Fields("selected_country"))
                .groupBy(new Fields(COUNTRY))
                .aggregate(new Fields(COUNTRY, TOTAL_SALES, CUSTOMER),
                        new FirstN.FirstNSortedAgg(N, TOTAL_SALES, true),
                        new Fields(CUSTOMER, TOTAL_SALES));

//        topology.newDRPCStream(RetailAnalysis.TOP_CANCELED, localDRPC)
//                .stateQuery(totalVolume, new TupleCollectionGet(), new Fields(COUNTRY, TOTAL_CANCELLATIONS))
//                .groupBy(new Fields(COUNTRY))
//                .aggregate(new Fields(COUNTRY, TOTAL_CANCELLATIONS),
//                        new FirstN.FirstNSortedAgg(N, TOTAL_CANCELLATIONS, false),
//                        new Fields(TOTAL_CANCELLATIONS));

        return topology.build();
    }
}
