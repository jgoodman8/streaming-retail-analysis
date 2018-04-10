package com.trident.retail_analysis;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SplitCSV extends BaseFunction {
    private static final String REGEX = ",";

    public void execute(TridentTuple tuple, TridentCollector collector) {
        for (String word : tuple.getString(0).split(REGEX)) {
            if (word.length() > 0) {
                collector.emit(new Values(word));
            }
        }
    }
}
