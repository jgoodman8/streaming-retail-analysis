#!/bin/bash

chmod +755 execute.sh
./execute.sh es.dmr.uimp.clustering.TrainInvoices ../resources/training.csv ./clustering ./threshold kMeans &
./execute.sh es.dmr.uimp.clustering.TrainInvoices ../resources/training.csv ./clustering_bisect ./threshold_bisect BisKMeans &