package it.escopelliti

import it.escopelliti.domain.DataValues._
import it.escopelliti.io.{CrossSalesInputData, DataLayer, HomologationsInputData, ProductsInputData}
import it.escopelliti.pipeline.TimeSeriesStatsStep
import it.escopelliti.utils.{ConfigurationProvider, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object TimeSeriesExtractionJob extends Logging {

  def main(args: Array[String]) {

    val sparkConf: SparkConf = new SparkConf()
      .setAppName("TimeSeriesExtractionJob")
      .setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    implicit val sqlContext = new SQLContext(sc)

    // We are assuming that trim and cleanings have been performed in the previous job
    val homologations: RDD[Homologation] = HomologationsInputData.apply(ConfigurationProvider.getHomologationPath())

    val products: RDD[ProductItem] = ProductsInputData
      .getProductsOfInterest(
        ProductsInputData.apply(ConfigurationProvider.getProductsPath()),
        homologations)

    val sales: RDD[(String, CrossSale)] = CrossSalesInputData.getSalesOfInterest(
      CrossSalesInputData.apply(ConfigurationProvider.getSalesPath()),
      products)
      .keyBy(_.productCode)
      .persist(StorageLevel.MEMORY_AND_DISK_SER)

    val completeOutputData: RDD[(String, Output)] = products
      .map {
        case p =>
          (p.id, Output(p, TimeSeries()))
      }

    val output = TimeSeriesStatsStep.apply(completeOutputData, sales)

    //query or show or use data or....write them
    DataLayer.write(output, ConfigurationProvider.getOutputPath())
  }
}
