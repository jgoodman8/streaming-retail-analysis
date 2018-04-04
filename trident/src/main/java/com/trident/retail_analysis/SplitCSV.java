package com.trident.retail_analysis;

import org.apache.storm.trident.operation.BaseFunction;
import org.apache.storm.trident.operation.TridentCollector;
import org.apache.storm.trident.tuple.TridentTuple;
import org.apache.storm.tuple.Values;

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
