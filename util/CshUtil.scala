package org.apache.spark.util

import scala.reflect.{ClassTag, classTag}

object CshUtil {
  def dump[T: ClassTag](t: T): String = {
    "%s: %s".format(t, classTag[T])
  }
}
