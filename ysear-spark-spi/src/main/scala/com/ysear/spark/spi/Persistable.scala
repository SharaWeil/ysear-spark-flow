package com.ysear.spark.spi

import org.apache.commons.lang.StringUtils
import org.apache.spark.internal.Logging
import org.apache.spark.storage.StorageLevel

import java.util.Optional

/**
 * @ClassName Persistable.scala
 * @createTime 2022年12月26日 14:59:00
 */
class Persistable extends Serializable with Logging {
  private final val PERSIST_STORAGES: List[String] = List(
    "MEMORY_ONLY",
    "DISK_ONLY",
    "MEMORY_AND_DISK",
    "MEMORY_AND_DISK_SER")

  private[this] var _cached: Boolean = false

  private[this] var _storageLevel: String = _

  def storageLevel: String = _storageLevel

  def storageLevel_=(value: String): Unit = {
    if (StringUtils.isBlank(value)) {
      return
    }
    val upCase: String = value.toUpperCase()
    if (!this.PERSIST_STORAGES.contains(upCase)) {
      throw new IllegalArgumentException("unsupported storageLevel " + storageLevel)
    }
    _storageLevel = upCase
    _cached = true
  }

  def cached: Boolean = _cached

}
