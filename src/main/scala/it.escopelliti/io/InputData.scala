package it.escopelliti.io

import it.escopelliti.domain.DataValues.{CrossSale, Homologation, ProductItem}
import it.escopelliti.utils.ConfigurationProvider
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

abstract class InputData[V: ClassTag]() extends Serializable {

  def apply(path: String)(implicit sc: SparkContext): RDD[V] =
    getInputData(DataLayer.read[V](path))

  protected def getInputData(data: RDD[V])
                            (implicit sc: SparkContext): RDD[V]

}


object ProductsInputData extends InputData[ProductItem] {

  override protected def getInputData(data: RDD[ProductItem])
                                     (implicit sc: SparkContext): RDD[ProductItem] = {

    //Put here some additional filter on data if needed
    data
  }

  def getProductsOfInterest(products: RDD[ProductItem], homologations: RDD[Homologation]) = {
    products
      .keyBy(_.id)
      .join(homologations.keyBy(_.productCode))
      .reduceByKey((a, _) => a)
      .map { case (_, (product, _)) => product }
  }
}

object HomologationsInputData extends InputData[Homologation] {

  override protected def getInputData(data: RDD[Homologation])
                                     (implicit sc: SparkContext): RDD[Homologation] = {
    val brand: String = ConfigurationProvider.getBrandOfInterest().toLowerCase
    data
      .filter {
        case homologation =>
          homologation.vehicle.brand.toLowerCase == brand
      }
  }
}

object CrossSalesInputData extends InputData[CrossSale] {

  override protected def getInputData(data: RDD[CrossSale])
                                     (implicit sc: SparkContext): RDD[CrossSale] = {
    val marketOfInterest: String = ConfigurationProvider.getMarketOfInterest().toLowerCase
    data
      .filter {
        case sale =>
          sale.nutsCode.toLowerCase.startsWith(marketOfInterest)
      }
  }

  def getSalesOfInterest(sales: RDD[CrossSale], productsOfInterests: RDD[ProductItem]) = {
    sales
      .keyBy(_.productCode)
      .join(productsOfInterests.keyBy(_.id))
      .reduceByKey((a, _) => a)
      .map { case (_, (product, _)) => product }


  }
}