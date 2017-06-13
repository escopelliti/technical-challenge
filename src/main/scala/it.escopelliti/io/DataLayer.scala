package it.escopelliti.io

import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag


object DataLayer {

  //it can be even tables. It does not matter. We put here a string as argument for convenience
  def read[V: ClassTag](path: String)(implicit sc: SparkContext) = {
    //read TBD
    //Please not consider this line.
    // For now we retrieve only fake RDD just to compile even if it does not make sense.
    sc.parallelize[V](Seq[V]())
  }

  def write[V: ClassTag](data: RDD[V], path: String) = Unit

}
