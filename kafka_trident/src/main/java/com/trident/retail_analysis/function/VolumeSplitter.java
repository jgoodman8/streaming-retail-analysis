package com.trident.retail_analysis.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class VolumeSplitter extends BaseFunction {

    @Override
    public void execute(TridentTuple tuple, TridentCollector collector) {
        float sales = 0.0f;
        float cancellations = 0.0f;

        float transactionPrice = tuple.getFloat(0);

        if (transactionPrice > 0) {
            sales = transactionPrice;
        } else {
            cancellations = transactionPrice * -1;
        }

        collector.emit(new Values(sales, cancellations));
    }

}
