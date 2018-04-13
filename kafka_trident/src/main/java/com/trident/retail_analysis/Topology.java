package com.trident.retail_analysis;

import backtype.storm.LocalDRPC;
import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.trident.retail_analysis.aggregator.VolumeReducer;
import com.trident.retail_analysis.function.*;
import storm.kafka.trident.TransactionalTridentKafkaSpout;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.*;
import storm.trident.testing.MemoryMapState;
import storm.trident.testing.Split;
import storm.trident.tuple.TridentTuple;

public class Topology {

    private static final int N = 5;
    private static final String KAFKA_INPUT = "str";
    private static final String DRPC_ARGUMENT = "args";
    private static final String STREAM_ID = "purchases_stream";

    private static final String COUNTRY = "Country";
    private static final String QUANTITY = "Quantity";
    private static final String CUSTOMER = "CustomerID";
    private static final String UNIT_PRICE = "UnitPrice";

    private static final String TOTAL_PRICE = "total_price";
    private static final String GROUPED_TRANSACTIONS = "grouped_transactions";

    private static final String SALES = "sales";
    private static final String CANCELLATIONS = "cancellations";

    private static final String TOTAL_SALES = "total_sales";
    private static final String TOTAL_CANCELLATIONS = "total_cancellations";

    private static final String[] REQUIRED_FIELDS = {QUANTITY, UNIT_PRICE, CUSTOMER, COUNTRY};
    private static final String[] CSV_FIELDS = {"InvoiceNo", "StockCode", "Description", QUANTITY,
            "InvoiceDate", UNIT_PRICE, CUSTOMER, COUNTRY};

    public static StormTopology buildTopology(TransactionalTridentKafkaSpout kafkaSpout, LocalDRPC localDRPC) {
        TridentTopology topology = new TridentTopology();

        TridentState totalVolume = topology
                .newStream(STREAM_ID, kafkaSpout)
                .each(new Fields(KAFKA_INPUT), new SplitCSV(), new Fields(CSV_FIELDS))
                .project(new Fields(REQUIRED_FIELDS))
                .each(new Fields(CUSTOMER, COUNTRY), new FilterNull())
                .each(new Fields(QUANTITY, UNIT_PRICE), new TotalPrice(), new Fields(TOTAL_PRICE))
                .each(new Fields(TOTAL_PRICE), new VolumeSplitter(), new Fields(SALES, CANCELLATIONS))
                .groupBy(new Fields(CUSTOMER, COUNTRY))
                .persistentAggregate(new MemoryMapState.Factory(), new Fields(SALES, CANCELLATIONS),
                        new VolumeReducer(), new Fields(GROUPED_TRANSACTIONS))
                .parallelismHint(16);

        topology.newDRPCStream(RetailAnalysis.TOP_SOLD, localDRPC)
                .each(new Fields(DRPC_ARGUMENT), new Split(), new Fields("selected_country"))
                .stateQuery(totalVolume, new TupleCollectionGet(), new Fields(CUSTOMER, COUNTRY, GROUPED_TRANSACTIONS))
                .each(new Fields(GROUPED_TRANSACTIONS), new SalesFilter(), new Fields(TOTAL_SALES))
                .groupBy(new Fields(COUNTRY))
                .aggregate(new Fields(CUSTOMER, TOTAL_SALES),
                        new FirstN.FirstNSortedAgg(N, TOTAL_SALES, true),
                        new Fields(CUSTOMER, TOTAL_SALES));

        topology.newDRPCStream(RetailAnalysis.TOP_CANCELED, localDRPC)
                .each(new Fields(DRPC_ARGUMENT), new Split(), new Fields("selected_country"))
                .stateQuery(totalVolume, new TupleCollectionGet(), new Fields(CUSTOMER, COUNTRY, GROUPED_TRANSACTIONS))
                .each(new Fields(GROUPED_TRANSACTIONS), new CancellationFilter(), new Fields(TOTAL_CANCELLATIONS))
                .groupBy(new Fields(COUNTRY))
                .aggregate(new Fields(CUSTOMER, TOTAL_CANCELLATIONS),
                        new FirstN.FirstNSortedAgg(N, TOTAL_CANCELLATIONS, true),
                        new Fields(CUSTOMER, TOTAL_CANCELLATIONS));

        return topology.build();
    }
}

class DebugFilter extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        String customer = tuple.getString(0);
        float xxx = tuple.getFloat(1);

        collector.emit(new Values(customer));
    }
}
