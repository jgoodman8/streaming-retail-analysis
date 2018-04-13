package com.trident.retail_analysis;

import storm.trident.operation.ReducerAggregator;
import storm.trident.tuple.TridentTuple;

public class VolumeReducer implements ReducerAggregator<VolumeState> {
    
    @Override
    public VolumeState init() {
        return new VolumeState();
    }

    @Override
    public VolumeState reduce(VolumeState volumeState, TridentTuple tuple) {
        float sales = tuple.getFloat(0);
        float cancellations = tuple.getFloat(1);

        volumeState.addSale(sales);
        volumeState.addCancellation(cancellations);

        return volumeState;
    }
}
