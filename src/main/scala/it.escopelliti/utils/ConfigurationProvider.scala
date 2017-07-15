package it.escopelliti.utils

import java.time.LocalDate
import java.time.format.DateTimeFormatter

import com.typesafe.config.ConfigFactory

import scala.util.{Failure, Success, Try}


object ConfigurationProvider {

  val config = ConfigFactory.load()

  val basePath = "timeseries-job"

  def getOutputPath() = config.getString(s"$basePath.output-path")

  def getRunDate(): LocalDate = {
    Try(DataProcessingUtils.parseDate(config.getString(s"$basePath.run-date"))) match {
      case Success(s) => s
      case Failure(f) => LocalDate.now
    }
  }

  def getSalesPath(): String = config.getString(s"$basePath.sales-path")

  def getHomologationPath(): String = config.getString(s"$basePath.homologation-path")

  def getProductsPath(): String = config.getString(s"$basePath.product-path")

  def getMarketOfInterest() = config.getString(s"$basePath.market")

  def getBrandOfInterest() = config.getString(s"$basePath.brand")

  def getTopSellerNum() = config.getInt(s"$basePath.sellers")
}
