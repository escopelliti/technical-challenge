package it.escopelliti.utils

import java.util.Date


object ConfigurationProvider {

  def getOutputPath() = "outputPath"

  def getRunDate(): Date = new Date()

  def getSalesPath(): String = "salesPath"

  def getHomologationPath(): String = "homologationPath"

  def getProductsPath(): String = "productPath"

  def getMarketOfInterest() = "market/geo"

  def getBrandOfInterest() = "brand"

  def getTopSellerNum() = 10
}
