package com.trident.retail_analysis.function;

import backtype.storm.tuple.Values;
import com.trident.retail_analysis.aggregator.VolumeState;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SalesFilter extends BaseFunction {

    public void execute(TridentTuple tuple, TridentCollector collector) {
        VolumeState volumeState = (VolumeState) tuple.get(0);

        collector.emit(new Values(volumeState.getSales()));
    }
}
