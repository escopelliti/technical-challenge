package it.escopelliti.domain

import java.time.Month
import java.util.Date

import it.escopelliti.utils.ConfigurationProvider
import org.apache.commons.math3.stat.descriptive.moment.StandardDeviation


object DataValues {

  import scala.collection.immutable.TreeMap

  case class Vehicle(group: String, brand: String, model: String, modelCode: String)

  case class ProductItem(id: String, name: String)

  case class Homologation(productCode: String, vehicle: Vehicle)

  object Stats {

    def apply(month: Month)(list: List[CrossSale]) = {

      val quantities = list.map(_.quantity)

      val sum = computeSum(quantities)
      val count = computeCount(quantities)
      val avg = computeAvg(sum, count)
      val std = computeStd(quantities, avg)
      val (min, max) = takeMinMax(quantities)
      val topSellers = retrieveTopSellers(list, ConfigurationProvider.getTopSellerNum())

      new Stats(month, sum, count, avg, std, min, max, topSellers)
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

    private def takeMinMax(list: List[Long]) = {
      (list.filter(_ <= 0).min, list.max)
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
        .toSet
    }
  }

  case class Stats(month: Month, totalSales: Long, transactionsCount: Long, averageSales: Double, std: Double,
                   minSale: Long, maxSale: Long, topSellers: Set[String])

  case class CrossSale(date: Date, productCode: String, sellerName: String, sellerTown: String,
                       sellerID: String, // built in the first job
                       country: String, nutsCode: String, quantity: Long, pricePerUnit: Double)

  case class TimeSeries(months: TreeMap[Int, Stats] = TreeMap.empty[Int, Stats])

  case class Output(product: Product, timeSeries: TimeSeries)

}
