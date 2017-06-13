package it.escopelliti.utils

import org.apache.log4j.Logger

trait Logging extends Serializable {

  @transient protected val log = Logger.getLogger(getClass.getName)

}
