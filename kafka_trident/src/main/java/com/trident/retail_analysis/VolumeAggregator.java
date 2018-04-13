package com.trident.retail_analysis;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class VolumeAggregator extends BaseAggregator<VolumeState> {
    @Override
    public VolumeState init(Object batchId, TridentCollector collector) {
        return new VolumeState();
    }

    @Override
    public void aggregate(VolumeState volumeState, TridentTuple tuple, TridentCollector collector) {
        float transactionPrice = tuple.getFloat(0);

        if (transactionPrice > 0) {
            volumeState.addSale(transactionPrice);
        } else {
            volumeState.addCancellation(transactionPrice);
        }
    }

    @Override
    public void complete(VolumeState volumeState, TridentCollector collector) {
        float totalSales = volumeState.getSales();
        float totalCancellations = volumeState.getCancellations();

        collector.emit(new Values(totalSales, totalCancellations));
    }
}
