# Streaming retail analysis with Apache Spark

## Requirements

- Java 8
- Apache Spark 2.7
- Apache Kafka
- SBT ([check instalation tutorial](https://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Linux.html))

## Setup

Sample of *Spark* installation in *standalone* mode:

```{bash}
wget https://archive.apache.org/dist/spark/spark-2.0.0/spark-2.0.0-bin-hadoop2.7.tgz
tar xvf spark-2.0.0-bin-hadoop2.7.tgz
rm spark-2.0.0-bin-hadoop2.7.tgz
mv spark-2.0.0-bin-hadoop2.7/ /opt/spark-2.0.0-bin-hadoop2.7
echo 'export PATH=$PATH:/opt/spark-2.0.0-bin-hadoop2.7/bin' >> ~/.bashrc
source ~/.bashrc
```

Project build and self-contained package generation:

```{bash}
sbt assembly
```

## Execution

### Model train

First, a fit for kMeans y Bisection kMeans models should be performed.

```{bash}
chmod +x start_training.sh
./start_traning.sh
```

Once the traning is over, the following folders and files should have been created:
- clustering/
- clustering_bisect/
- threshold
- threshold_bisect

### Streaming run

Streaming pipeline application execution:

```{bash}
chmod +x start_pipeline.sh
./start_pipeline.sh
```

Once the streaming analysis application is running, we may run the purchasses simulator:

```{bash}
chmod +x productiondata.sh
./productiondata.sh ../resources/retail.csv purchases
```

### Monitoring

The information created/extrated by the streaming pipeline is written into four *Kafka* topics, as the architecture diagram shows. The created topics are named as:

- cancelaciones
- facturas_erroneas
- anomalias_kmeans
- anomalias_bisect_kmeans
