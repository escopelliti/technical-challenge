package it.escopelliti.io

import it.escopelliti.domain.DataValues.Output
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext

object DataLayer {

  def read(path: String)(implicit sqlContext: SQLContext) = {

    sqlContext.read.parquet(path)
  }

  def write(data: RDD[(String, Output)], path: String)(implicit sqlContext: SQLContext) = {

    import sqlContext.implicits._
    data.toDF.write.format("com.databricks.spark.csv").save(path)
  }

}
