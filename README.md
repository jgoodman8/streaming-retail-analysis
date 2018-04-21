# Práctica de streaming

## Trident

### Preparación del entorno

Para la ejecución de esta práctica, debemos crear previamente el *topic* de  Kafka 'compras-trident'. Para ello, nos conectamos al *host* *node1* por ssh con el usuario y contraseña *root/hadoop*.

```
ssh root@node1
kafka-topics.sh —create —topic compras-trident —partitions 1 —replication 1 —zookeeper namenode:2181
```

A continuación, comprobamos que el topic haya sido creado.

```
kafka-topics.sh --list --zookeeper namenode:2181
```

Finalmente, necesitamos configurar el entorno del simulador de compras. Para ello, se utilizará el archivo `resources/online_retail.csv`. Dado que no necesitamos la cabecera del archivo, procedemos a eliminarla.

```
tail -n +2 resources/online_retail.csv > resources/retail.csv
```

Es posible que sea necesario actualizar la versión de Java, asi como instalar Maven.

```{bash}
sudo add-apt-repository ppa:openjdk-r/ppa
sudo apt-get update
sudo apt-get install openjdk-8-jre
sudo update-alternatives --config java
# seleccionar la versión 8 de java

apt-get install maven
```

Así mismo, para compilar dicho simulador, necesitamos tener instalado *sbt* ([vease el manual de instalación de *sbt*](https://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Linux.html)).
 
```
cd spark
sbt assembly
chmod +755 productiondata.sh

./productiondata.sh ../resources/retail.csv compras-trident
```

### Ejecución

Nos colocamos en el directorio `kafka_trident/` y compilamos el 
código utilizando *gradle*.

```
cd kafka_trident

gradle clean build
```

Finalmente, ejecutamos el archivo generado. Observamos que como primer argumento, toma una lista de países (entre  comillas y separados por comas) que se utilizarán, para filtrar los resutados. Como segundo parámetro, toma el número de iteraciones (y por lo tanto de resultados) que se mostrarán. 

```
java -jar purchases_trident_kafka.jar "France,Portugal,United Kingdom" 10000
```

Como salida, cada segundo se muestran los cinco clientes, de los países seleccionados, con mayor numero de cancelaciones y de ventas. Además se muestra el montante total facturado por cada cliente.

## Spark Streaming

El contenido de esta práctica se encuentra en el fichero `spark_streaming`.

### Preparación del entorno

Es posible que sea necesario actualizar la versión de Java, asi como instalar Maven.

```{bash}
sudo add-apt-repository ppa:openjdk-r/ppa
sudo apt-get update
sudo apt-get install openjdk-8-jre
sudo update-alternatives --config java
# seleccionar la versión 8 de java

apt-get install maven
```

Antes de comenzar con la ejecución, debemos compilar el código. Para dicha compilación, necesitamos tener instalado *sbt* ([vease el manual de instalación de *sbt*](https://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Linux.html)).

```{bash}
cd spark_streaming
sbt assembly
```

Finalmente, es necesario instalar Spark. Lo realizaremos en modo *standalone*.

```{bash}
wget https://archive.apache.org/dist/spark/spark-2.0.0/spark-2.0.0-bin-hadoop2.7.tgz
tar xvf spark-2.0.0-bin-hadoop2.7.tgz
rm spark-2.0.0-bin-hadoop2.7.tgz
mv spark-2.0.0-bin-hadoop2.7/ /opt/spark-2.0.0-bin-hadoop2.7
echo 'export PATH=$PATH:/opt/spark-2.0.0-bin-hadoop2.7/bin' >> ~/.bashrc
source ~/.bashrc
```

### Ejecución

En primer lugar, debemos de ejecutar el entrenamiento de los modelos que queremos comparar: kMeans y Bisection kMeans. Para ello, ejecutamos el archivo start_training.sh.

```{bash}
chmod +x start_training.sh
./start_traning.sh
```

Una vez terminado el entrenamiento, deberían de haberse creado los siguientes archivos y ficheros:
- clustering/
- clustering_bisect/
- threshold
- threshold_bisect

Ahora procedemos a lanzar el pipeline con el comando start_pipeline. 

```{bash}
chmod +x start_pipeline.sh
./start_pipeline.sh
```

Una vez lanzado el pipeline, procedemos a simular compras y escribirlas en un topic de Kafka. Para ello, utilizamos el siguiente comando:

```{bash}
chmod +x productiondata.sh
./productiondata.sh ../resources/retail.csv purchases
```

En dicho pipeline, procederemos a consumir mensajes de un topic de Kafka y realizar determinados procesados para escribir en los siguientes topics:

- cancelaciones
- facturas_erroneas
- anomalias_kmeans
- anomalias_bisect_kmeans

Para comprobar si, efectivamente, se están generando resultados en estos topics. Podemos abrir una terminar para cada topic consumir los mensajes. A continuación, se describe el proceso necesario para consumir los mensajes del topic cancelaciones (será lo mismo para los demás topics cambiando el nombre).

```{bash}
ssh root@node1
kafka-console-consumer.sh --zookeeper namenode:2181 -topic cancelaciones
```
