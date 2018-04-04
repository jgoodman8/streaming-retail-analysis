package com.trident.retail_analysis;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

public class TotalPrice extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {
        int quantity = tuple.getInteger(0);
        int unitPrice = tuple.getInteger(1);

        collector.emit(new Values(quantity * unitPrice));
    }
}
