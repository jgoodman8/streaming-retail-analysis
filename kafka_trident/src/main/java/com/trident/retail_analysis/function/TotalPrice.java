package com.trident.retail_analysis.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class TotalPrice extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {
        try {
            int quantity = Integer.parseInt(tuple.getString(0));
            float unitPrice = Float.parseFloat(tuple.getString(1));

            float totalPrice = quantity * unitPrice;

            collector.emit(new Values(totalPrice));
        } catch (NumberFormatException exception) {
            System.out.println(exception.getMessage());
        }
    }
}
