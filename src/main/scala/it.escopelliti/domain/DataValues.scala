package it.escopelliti.domain

import java.time.{LocalDate, Month}

import it.escopelliti.utils.{ConfigurationProvider, DataProcessingUtils}
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation
import org.apache.spark.sql.Row


object DataValues {

  import scala.collection.immutable.TreeMap

  case class Vehicle(group: String, brand: String, model: String, modelCode: String)

  object Vehicle {
    def apply(str: String) = {

      val vehiclePart = str.trim.split(":")
      new Vehicle(
        vehiclePart.head,
        vehiclePart(1),
        vehiclePart(2),
        vehiclePart.last
      )
    }
  }

  case class ProductItem(id: String, name: String)

  object ProductItem {
    def apply()(row: Row) = {
      new ProductItem(
        row.getAs[String]("id"),
        row.getAs[String]("name")
      )
    }
  }

  case class Homologation(productCode: String, vehicle: Vehicle)

  object Homologation {
    def apply()(row: Row) = {
      new Homologation(
        row.getAs[String]("productCode"),
        Vehicle(row.getAs[String]("vehicle"))
      )
    }
  }

  object Stats {

    def apply(month: Month)(list: List[CrossSale]) = {

      val quantities = list.map(_.quantity)

      val sum = computeSum(quantities)
      val count = computeCount(quantities)
      val avg = computeAvg(sum, count)
      val std = computeStd(quantities, avg)
      val (min, max) = takeMinMax(quantities)
      val topSellers = retrieveTopSellers(list, ConfigurationProvider.getTopSellerNum())

      new Stats(month.name, sum, count, avg, std, min, max, topSellers)
    }

    // Draft implementation for those. Here you can use some library or turn the job to make it using dataframe
    // to aggregate more simply

    //Pay attention you are not excluding negative quantity
    private def computeSum(list: List[Long]) = list.sum

    private def computeCount(list: List[Long]) = list.size

    private def computeAvg(sum: Double, count: Long) = sum / count

    private def computeStd(list: List[Long], avg: Double) = {
      val std = new StandardDeviation()
      std.evaluate(list.map(_.toDouble).toArray, avg)
    }

    def emptyStats(month: Month) = new Stats(month.name, 0, 0, 0, 0, 0, 0, List.empty[String])

    private def takeMinMax(list: List[Long]) = {
      (list.filterNot(_ <= 0).min, list.max)
    }

    // you can do better than this
    private def retrieveTopSellers(list: List[CrossSale], num: Int) = {

      list
        .groupBy(_.sellerID)
        .map {
          case (sellerID, salesList) =>
            (sellerID, salesList.map(_.quantity).sum)
        }
        .toList
        .sortBy(_._2)
        .reverse
        .map(_._1) // take only sellerID
        .take(num)
        .distinct
    }
  }

  case class Stats(month: String, totalSales: Long, transactionsCount: Long, averageSales: Double, std: Double,
                   minSale: Long, maxSale: Long, topSellers: List[String] = List.empty[String])

  case class CrossSale(date: LocalDate, productCode: String, sellerName: String, sellerTown: String,
                       sellerID: String, // built in the first job
                       country: String, nutsCode: String, quantity: Long, pricePerUnit: Double)

  object CrossSale {

    def apply()(row: Row) = {

      new CrossSale(
        DataProcessingUtils.parseDate(row.getAs[String]("date")),
        row.getAs[String]("productCode"),
        row.getAs[String]("sellerName"),
        row.getAs[String]("sellerTown"),
        row.getAs[String]("sellerID"),
        row.getAs[String]("country"),
        row.getAs[String]("nutsCode"),
        row.getAs[String]("quantity").toLong,
        row.getAs[String]("pricePerUnit").toDouble
      )
    }
  }

  case class TimeSeries(months: TreeMap[Int, Stats] = TreeMap.empty[Int, Stats])

  case class Output(product: ProductItem, timeSeries: TimeSeries)

}
