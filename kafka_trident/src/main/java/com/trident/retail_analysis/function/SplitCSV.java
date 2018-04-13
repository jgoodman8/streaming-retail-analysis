package com.trident.retail_analysis.function;

import backtype.storm.tuple.Values;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.TridentCollector;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.List;

public class SplitCSV extends BaseFunction {

    private static final String REGEX = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)";

    public void execute(TridentTuple tuple, TridentCollector collector) {
        List words = new ArrayList<>();

        for (String word : tuple.getString(0).split(REGEX, -1)) {
            words.add(word);
        }

        collector.emit(new Values(words.toArray()));
    }
}
