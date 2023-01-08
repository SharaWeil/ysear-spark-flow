package com.ysear.spark.core.listener

import org.apache.spark.executor.{InputMetrics, OutputMetrics, TaskMetrics}

import java.sql.Timestamp
import scala.collection.mutable

/**
 * @ClassName FlowSparkListener.scala
 * @createTime 2022年12月27日 11:36:00
 */
class FlowSparkListener extends FlowSparkListenerAdapter {

  import org.apache.spark.scheduler.{SparkListenerApplicationEnd, SparkListenerJobStart, SparkListenerTaskEnd, SparkListenerTaskStart}

  import java.util.UUID
  import java.util.concurrent.atomic.AtomicLong

  val startTime: Long = System.currentTimeMillis

  val uuid: String = UUID.randomUUID.toString

  private val inputBytes = new AtomicLong(0L)

  private val inputRecords = new AtomicLong(0L)

  private val outputBytes = new AtomicLong(0L)

  private val outputRecords = new AtomicLong(0L)

  private val jobCounter = new AtomicLong(0L)

  private val taskCounter = new AtomicLong(0L)

  def flowInfo: mutable.HashMap[String,Object] = {
    val info = new mutable.HashMap[String,Object]()
    info.put("uuid", this.uuid)
    info.put("starttime", new Timestamp(this.startTime))
    info.put("endtime", new Timestamp(System.currentTimeMillis))
    info.put("inputBytes", java.lang.Long.valueOf(this.inputBytes.get))
    info.put("inputRecords",java.lang.Long.valueOf(this.inputRecords.get))
    info.put("outputBytes", java.lang.Long.valueOf(this.outputBytes.get))
    info.put("outputRecords", java.lang.Long.valueOf(this.outputRecords.get))
    info.put("jobCount", java.lang.Long.valueOf(this.jobCounter.get))
    info.put("taskCount", java.lang.Long.valueOf(this.taskCounter.get))
    info
  }

  override def onTaskEnd(taskEnd: SparkListenerTaskEnd): Unit = {
    super.onTaskEnd(taskEnd)
    val taskMetrics: TaskMetrics = taskEnd.taskMetrics
    if (taskMetrics == null) return
    val inputMetrics: InputMetrics = taskMetrics.inputMetrics
    val outputMetrics: OutputMetrics = taskMetrics.outputMetrics
    if (inputMetrics != null) {
      this.inputBytes.addAndGet(inputMetrics.bytesRead)
      this.inputRecords.addAndGet(inputMetrics.recordsRead)
    }
    if (outputMetrics != null) {
      this.outputBytes.addAndGet(outputMetrics.bytesWritten)
      this.outputRecords.addAndGet(outputMetrics.recordsWritten)
    }
  }

  override def onTaskStart(taskStart: SparkListenerTaskStart): Unit = {
    super.onTaskStart(taskStart)
    this.jobCounter.incrementAndGet
  }

  override def onJobStart(jobStart: SparkListenerJobStart): Unit = {
    super.onJobStart(jobStart)
    this.taskCounter.incrementAndGet
  }

  override def onApplicationEnd(applicationEnd: SparkListenerApplicationEnd): Unit = {
    super.onApplicationEnd(applicationEnd)
    ("total job count: " + this.jobCounter.get)
    log.info("total task count: " + this.taskCounter.get)
    log.info("total bytes read: " + this.inputBytes.get)
    log.info("total records read: " + this.inputRecords.get)
    log.info("total bytes written: " + this.outputBytes.get)
    log.info("total records written: " + this.outputRecords.get)
  }
}
