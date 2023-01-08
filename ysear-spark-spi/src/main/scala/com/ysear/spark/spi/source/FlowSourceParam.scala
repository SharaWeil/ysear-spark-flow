package com.ysear.spark.spi.source

import com.ysear.spark.spi.BaseParam

/*
 * @Author Administrator
 * @Date 2023/1/6
 **/
class FlowSourceParam(val `type`: String,
                      val sourceId: String,
                      val alias: String,
                      val querySql: String) extends BaseParam(`type`, sourceId) {

  /**
   * 默认并行度
   */
  private[this] var _partition: Int = 10


  private[this] var _cacheLevel: String = "MEMORY_ONLY"


  def partition: Int = _partition

  def partition_=(value: Int): Unit = {
    _partition = value
  }

  def cacheLevel: String = _cacheLevel

  def cacheLevel_=(value: String): Unit = {
    _cacheLevel = value
  }

}
