package com.trident.retail_analysis.query;

import storm.trident.operation.TridentCollector;
import storm.trident.state.BaseQueryFunction;
import storm.trident.state.ITupleCollection;
import storm.trident.state.State;
import storm.trident.tuple.TridentTuple;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CountryQuery extends BaseQueryFunction<State, Iterator<List<Object>>> {
    @Override
    public List<Iterator<List<Object>>> batchRetrieve(State state, List<TridentTuple> args) {
        List<Iterator<List<Object>>> retrieve = new ArrayList(args.size());

        if(args.size() > 1) {
            System.out.printf("asdasdasd");
        }

        for (int i = 0; i < args.size(); i++) {
            String selectedCountry = (String) args.get(i).get(0);

            Iterator<List<Object>> tuplesIterator = ((ITupleCollection) state).getTuples();
            List<List<Object>> filteredTuples = new ArrayList<>();

            while (tuplesIterator.hasNext()) {
                List<Object> tuples = tuplesIterator.next();
                String country = (String) tuples.get(1);

                if (country.equals(selectedCountry)) {
                    filteredTuples.add(tuples);
                }
            }

            retrieve.add(filteredTuples.iterator());
        }

        return retrieve;
    }

    @Override
    public void execute(TridentTuple tuple, Iterator<List<Object>> tuplesIterator, TridentCollector collector) {
        while(tuplesIterator.hasNext()) {
            collector.emit(tuplesIterator.next());
        }
    }
}
