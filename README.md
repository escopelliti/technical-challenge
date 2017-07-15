# TimeSeriesExtractionJob

Draft implementation of a Spark job which performs aggregation and transformation of input data in order to extract monthly timeseries for both brand and market of interest for manufacturers. It expects to process sales' and product's input data.
Some custom implementations have to be done to get one's bearings with your data structures (in the development some assumptions have been done) while the aggregation and enrichment logic remain almost the same even changing the use cases.

## Getting Started

Get [Gradle](https://gradle.org/install) or use gradle wrapper (if you are using Intellij IDEA)

### Installing

1. Clone the project
2. Build (from the project root):
```
gradle clean build
```
3. Deploy artifact
4. Write configuration file to be passed as argument to the job. At least you have to specify both market/geo and brand on which perform the aggregations.
5. Launch spark-submit
```
${SPARK_HOME}/bin/spark-submit \
  --class it.escopelliti.TimeSeriesExtractionJob \
  --master ${RESOURCE_MGR} \
  --num-executors $NUM_EXECUTORS \
  --executor-memory $EXECUTOR_MEMORY \
  --executor-cores $EXECUTOR_CORES \
  --driver-memory $DRIVER_MEMORY \
   ${SCRIPT_DIR}/libs/
```

## Description

### Package **domain**

It includes all the data structures used in the code such as TimeSeries representation and Stats as well

### Package **io**

It contains ```InputData``` object which implements the basic aggregation and trasformation on data read by ```DataLayer```.

### Package **pipeline**

Here you may find the aggregation and trasformation step able to generate time series.

### Package **utils**

That's a common used package in which you may find some ```DataProcessingUtils``` and the ```ConfigurationProvider``` object which keeps all the configuration info including those passed as arguments to the job.

### Bootstrap **TimeSeriesExtractionJob**

Entry point of the Spark Job.

## Built With

* [Gradle](https://gradle.org/) - Dependency Management
* [Spark](https://spark.apache.org/) - Open Source cluster computing framework
* [Scala](https://www.scala-lang.org/) - general purpose programming language

## Authors

* **Enrico Scopelliti**

## Contributions

Please contribute by building configuration and I/O part.

## License

This project is licensed under the MIT License - see [LICENSE](LICENSE) for details.
