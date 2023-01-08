package com.ysear.spark.core.listener

import org.apache.spark.internal.Logging
import org.apache.spark.scheduler._

import scala.collection.mutable


/**
 * @ClassName AbsListener.scala
 * @createTime 2022年12月27日 11:30:00
 */
class AbsListener extends SparkListener with Logging{

  private val listenerList = new mutable.ListBuffer[AbsListener]()

  override def onStageCompleted(sparkListenerStageCompleted: SparkListenerStageCompleted): Unit = {
    if (sparkListenerStageCompleted != null) {
      for (absListener <- this.listenerList) {
        absListener.onStageCompleted(sparkListenerStageCompleted)
      }
    }
  }

  override def onStageSubmitted(sparkListenerStageSubmitted: SparkListenerStageSubmitted): Unit = {
    if (sparkListenerStageSubmitted != null) {
      for (absListener <- this.listenerList) {
        absListener.onStageSubmitted(sparkListenerStageSubmitted)
      }
    }
  }

  override def onTaskStart(sparkListenerTaskStart: SparkListenerTaskStart): Unit = {
    if (sparkListenerTaskStart != null) {
      for (absListener <- this.listenerList) {
        absListener.onTaskStart(sparkListenerTaskStart)
      }
    }
  }

  override def onTaskGettingResult(sparkListenerTaskGettingResult: SparkListenerTaskGettingResult): Unit = {
    if (sparkListenerTaskGettingResult != null) {
      for (absListener <- this.listenerList) {
        absListener.onTaskGettingResult(sparkListenerTaskGettingResult)
      }
    }
  }

  override def onTaskEnd(sparkListenerTaskEnd: SparkListenerTaskEnd): Unit = {
    if (sparkListenerTaskEnd != null) {
      for (absListener <- this.listenerList) {
        absListener.onTaskEnd(sparkListenerTaskEnd)
      }
    }
  }

  override def onJobStart(sparkListenerJobStart: SparkListenerJobStart): Unit = {
    if (sparkListenerJobStart != null) {
      for (absListener <- this.listenerList) {
        absListener.onJobStart(sparkListenerJobStart)
      }
    }
  }

  override def onJobEnd(sparkListenerJobEnd: SparkListenerJobEnd): Unit = {
    if (sparkListenerJobEnd != null) {
      for (absListener <- this.listenerList) {
        absListener.onJobEnd(sparkListenerJobEnd)
      }
    }
  }

  override def onEnvironmentUpdate(sparkListenerEnvironmentUpdate: SparkListenerEnvironmentUpdate): Unit = {
    if (sparkListenerEnvironmentUpdate != null) {
      for (absListener <- this.listenerList) {
        absListener.onEnvironmentUpdate(sparkListenerEnvironmentUpdate)
      }
    }
  }

  override def onBlockManagerAdded(sparkListenerBlockManagerAdded: SparkListenerBlockManagerAdded): Unit = {
    if (sparkListenerBlockManagerAdded != null) {
      for (absListener <- this.listenerList) {
        absListener.onBlockManagerAdded(sparkListenerBlockManagerAdded)
      }
    }
  }

  override def onBlockManagerRemoved(sparkListenerBlockManagerRemoved: SparkListenerBlockManagerRemoved): Unit = {
    if (sparkListenerBlockManagerRemoved != null) {
      for (absListener <- this.listenerList) {
        absListener.onBlockManagerRemoved(sparkListenerBlockManagerRemoved)
      }
    }
  }

  override def onBlockUpdated(blockUpdated: SparkListenerBlockUpdated): Unit = {
    if (blockUpdated != null) {
      for (absListener <- this.listenerList) {
        absListener.onBlockUpdated(blockUpdated)
      }
    }
  }

  override def onUnpersistRDD(sparkListenerUnpersistRDD: SparkListenerUnpersistRDD): Unit = {
    if (sparkListenerUnpersistRDD != null) {
      for (absListener <- this.listenerList) {
        absListener.onUnpersistRDD(sparkListenerUnpersistRDD)
      }
    }
  }

  override def onApplicationStart(sparkListenerApplicationStart: SparkListenerApplicationStart): Unit = {
    if (sparkListenerApplicationStart != null) {
      for (absListener <- this.listenerList) {
        absListener.onApplicationStart(sparkListenerApplicationStart)
      }
    }
  }

  override def onApplicationEnd(sparkListenerApplicationEnd: SparkListenerApplicationEnd): Unit = {
    if (sparkListenerApplicationEnd != null) {
      for (absListener <- this.listenerList) {
        absListener.onApplicationEnd(sparkListenerApplicationEnd)
      }
    }
  }

  override def onExecutorMetricsUpdate(sparkListenerExecutorMetricsUpdate: SparkListenerExecutorMetricsUpdate): Unit = {
    if (sparkListenerExecutorMetricsUpdate != null) {
      for (absListener <- this.listenerList) {
        absListener.onExecutorMetricsUpdate(sparkListenerExecutorMetricsUpdate)
      }
    }
  }

  override def onExecutorAdded(sparkListenerExecutorAdded: SparkListenerExecutorAdded): Unit = {
    if (sparkListenerExecutorAdded != null) {
      for (absListener <- this.listenerList) {
        absListener.onExecutorAdded(sparkListenerExecutorAdded)
      }
    }
  }

  override def onExecutorRemoved(sparkListenerExecutorRemoved: SparkListenerExecutorRemoved): Unit = {
    if (sparkListenerExecutorRemoved != null) {
      for (absListener <- this.listenerList) {
        absListener.onExecutorRemoved(sparkListenerExecutorRemoved)
      }
    }
  }

  def addListener(absListener: AbsListener): Unit = {
    this.listenerList.append(absListener)
  }

  def removeListener(absListener: AbsListener): Unit = {
    this.listenerList -= absListener
  }
}
