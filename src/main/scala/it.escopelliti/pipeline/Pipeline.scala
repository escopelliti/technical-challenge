package it.escopelliti.pipeline

import java.time.{LocalDate, Month}

import it.escopelliti.utils.ConfigurationProvider
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

trait Pipeline extends Serializable {

  protected def baseEnrichOutput[K: ClassTag, V: ClassTag, W: ClassTag](left: RDD[(K, V)])
                                                                       (right: RDD[(K, W)],
                                                                        month: Month,
                                                                        updateOutput: (V, Option[W], Month) => V): RDD[(K, V)] = {
    left
      .leftOuterJoin(right) // we do not know beforehand if there will be stats for that month
      .mapValues { case (prevOutput, valueOpt) =>
      updateOutput(prevOutput, valueOpt, month)
    }
  }

}

abstract class BasePipeline[K: ClassTag, V: ClassTag, T: ClassTag, W: ClassTag]() extends Pipeline {

  def apply(input: RDD[(K, V)], data: RDD[(K, T)]): RDD[(K, V)] = {
    build(input, data, ConfigurationProvider.getRunDate())
  }

  protected def transform(data: RDD[(K, T)], month: Month, runDate: LocalDate): RDD[(K, W)]

  protected def updateOutput(kpiOut: V, valueOpt: Option[W], month: Month): V

  protected def build(input: RDD[(K, V)], data: RDD[(K, T)], date: LocalDate): RDD[(K, V)] = {

    Month.values().foldLeft(input) {
      case (acc, month) =>
        baseEnrichOutput[K, V, W](acc)(transform(data, month, date), month, updateOutput)
    }
  }
}