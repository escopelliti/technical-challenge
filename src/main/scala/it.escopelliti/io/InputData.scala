package it.escopelliti.io

import it.escopelliti.domain.DataValues.{CrossSale, Homologation, ProductItem}
import it.escopelliti.utils.{ConfigurationProvider, Logging}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}

import scala.reflect.ClassTag

abstract class InputData[V: ClassTag]() extends Serializable {

  def apply(path: String)(implicit sqlContext: SQLContext): RDD[V] =
    getInputData(DataLayer.read(path))

  protected def getInputData(data: DataFrame)
                            (implicit sqlContext: SQLContext): RDD[V]

}


object ProductsInputData extends InputData[ProductItem] {

  override protected def getInputData(data: DataFrame)
                                     (implicit sqlContext: SQLContext): RDD[ProductItem] = {

    //Put here some additional filter on data if needed
    data
      .rdd
      .map(ProductItem())
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

  override protected def getInputData(data: DataFrame)
                                     (implicit sqlContext: SQLContext): RDD[Homologation] = {
    val brand: String = ConfigurationProvider.getBrandOfInterest().toLowerCase
    data
      .rdd
      .map(Homologation())
      .filter {
        case homologation =>
          homologation.vehicle.brand.toLowerCase == brand
      }
  }
}

object CrossSalesInputData extends InputData[CrossSale] with Logging {

  override protected def getInputData(data: DataFrame)
                                     (implicit sqlContext: SQLContext): RDD[CrossSale] = {
    val marketOfInterest: String = ConfigurationProvider.getMarketOfInterest().toLowerCase
    data
      .rdd
      .map(CrossSale())
      .filter {
        case sale =>
          sale.nutsCode.toLowerCase.startsWith(marketOfInterest)
      }
  }

  def getSalesOfInterest(sales: RDD[CrossSale], productsOfInterests: RDD[ProductItem]) = {
    sales
      .keyBy(_.productCode)
      .join(productsOfInterests.keyBy(_.id))
      .map { case (_, (product, _)) => product }


  }
}