package it.escopelliti.pipeline

import java.time.Month
import java.util.Date

import it.escopelliti.domain.DataValues.{CrossSale, Output, Stats, TimeSeries}
import org.apache.spark.rdd.RDD
import it.escopelliti.utils.DataProcessingUtils._

object TimeSeriesStatsStep {
  def apply: TimeSeriesStatsStep = new TimeSeriesStatsStep()
}

class TimeSeriesStatsStep() extends BasePipeline[String, Output, CrossSale, Stats] {

  def transform(data: RDD[(String, CrossSale)], month: Month, runDate: Date): RDD[(String, Stats)] = {

    data
      .filter {
        case (_, sale) =>
          val saleDate = sale.date
          isDateInMonth(saleDate, month) && isValidDate(saleDate, runDate)
      }
      .aggregateByKey(List.empty[CrossSale])(_ :+ _, _ ++ _)
      .mapValues(Stats(month))
  }

  override protected def updateOutput(previousOutput: Output, valueOpt: Option[Stats], month: Month): Output = {
    val newTimeSeries = valueOpt match {
      case Some(stats) =>
        previousOutput.timeSeries.months + (month.getValue() -> stats)
      case None =>
        previousOutput.timeSeries.months + (month.getValue() -> Stats(month)(List.empty[CrossSale]))
    }
    previousOutput.copy(timeSeries = TimeSeries(newTimeSeries))
  }
}
