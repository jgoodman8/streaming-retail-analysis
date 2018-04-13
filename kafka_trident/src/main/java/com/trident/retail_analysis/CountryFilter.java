package com.trident.retail_analysis;

import storm.trident.operation.BaseFilter;
import storm.trident.tuple.TridentTuple;

import java.util.Arrays;
import java.util.List;

public class CountryFilter extends BaseFilter {
    @Override
    public boolean isKeep(TridentTuple tuple) {
        List countries = Arrays.asList(tuple.getString(0).split(" "));
        String country = tuple.getString(1);

        boolean keep = countries.contains(country);

        return keep;
    }
}
