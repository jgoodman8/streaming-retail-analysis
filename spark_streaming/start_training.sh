#!/bin/bash

chmod +755 execute.sh
./execute.sh es.dmr.uimp.clustering.TrainInvoices ../resources/training.csv ./clustering ./thresholds kMeans &
./execute.sh es.dmr.uimp.clustering.TrainInvoices ../resources/training.csv ./clustering_bisect ./thresholds_bisect BisKMeans &