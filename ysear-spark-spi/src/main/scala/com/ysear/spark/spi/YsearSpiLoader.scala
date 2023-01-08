package com.ysear.spark.spi

import java.util
import scala.collection.mutable.ListBuffer
/*
 * @Author Administrator
 * @Date 2023/1/8
 **/
trait YsearSpiLoader {

  def load[S](service: Class[S]):ListBuffer[S]
}
