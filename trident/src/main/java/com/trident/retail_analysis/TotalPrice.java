package com.trident.retail_analysis;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TotalPrice extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {
        int quantity = tuple.getInteger(0);
        int unitPrice = tuple.getInteger(1);

        collector.emit(new Values(quantity * unitPrice));
    }
}
