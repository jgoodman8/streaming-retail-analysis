# Práctica de streaming

## Trident

### Preparación del entorno

Para la ejecución de esta práctica, debemos crear previamente el *topic* de 
Kafka 'compras-trident'. Para ello, nos conectamos al *host* *node1* por ssh con 
el usuario y contraseña *root/hadoop*.

```
ssh root@node1
kafka-topics.sh —create —topic compras-trident —partitions 1 —replication 1 —zookeeper namenode:2181
```

A continuación, comprobamos que el topic haya sido creado.

```
kafka-topics.sh --list --zookeeper namenode:2181
```

Finalmente, necesitamos configurar el entorno del simulador de compras. Para ello, se utilizará
el archivo `resources/online_retail.csv`. Dado que no necesitamos la cabecera del 
archivo, procedemos a eliminarla.

```
tail -n +2 resources/online_retail.csv > resources/retail.csv
```

Así mismo, para compilar dicho simulador, necesitamos 
tener instalado *sbt* ([vease el manual de instalación de *sbt*](https://www.scala-sbt.org/0.13/docs/Installing-sbt-on-Linux.html)).
 
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

Finalmente, ejecutamos el archivo generado. Observamos que como primer argumento, 
toma una lista de países (entre comillas y separados por comas) que se utilizarán,
para filtrar los resutados. Como segundo parámetro, toma el número de iteraciones 
(y por lo tanto de resultados) que se mostrarán. 

```
java -jar purchases_trident_kafka.jar "France,Portugal,United Kingdom" 10000
```

Como salida, cada segundo se muestran los cinco clientes, de los países seleccionados,
con mayor numero de cancelaciones y de ventas. Además se muestra el montante total facturado
por cada cliente.

## Arquitectura

 

