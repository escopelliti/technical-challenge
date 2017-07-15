package it.escopelliti.utils

import java.time.format.DateTimeFormatter
import java.time.{LocalDate, Month}


object DataProcessingUtils {

  def isDateInMonth(date: LocalDate, month: Month) = month == date.getMonth

  //to be implemented - just to check if the transaction date is before running date
  def isValidDate(date: LocalDate, compareDate: LocalDate) = {
    date.compareTo(compareDate) <= 0
  }

  def parseDate(stringDate: String): LocalDate = {
    val formatter = DateTimeFormatter.ofPattern("yyyyMMdd")
    LocalDate.parse(stringDate, formatter)
  }
}
