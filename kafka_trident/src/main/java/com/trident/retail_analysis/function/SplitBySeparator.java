package com.trident.retail_analysis.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

public class SplitBySeparator extends BaseFunction {

    private String separator;

    public SplitBySeparator(String separator) {
        this.separator = separator;
    }

    public void execute(TridentTuple tuple, TridentCollector collector) {
        for (String word : tuple.getString(0).split(separator)) {
            if (word.length() > 0) {
                collector.emit(new Values(word));
            }
        }
    }
}
