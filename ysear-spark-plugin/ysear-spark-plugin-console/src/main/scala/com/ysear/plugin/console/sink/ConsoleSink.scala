package com.ysear.plugin.console.sink

import com.ysear.spark.spi.sink.{AbsFlowSink, FlowSinkParam}
import org.apache.spark.sql.DataFrame

/*
 * @Author Administrator
 * @Date 2023/1/7
 **/
class ConsoleSink(sinkParam: FlowSinkParam) extends AbsFlowSink(viewName = sinkParam.asInstanceOf[ConsoleSinkParam].tableName) {

  private val consoleSinkParam: ConsoleSinkParam = sinkParam.asInstanceOf[ConsoleSinkParam]

  override def sink(dataFrame: DataFrame): Unit = {
    val rowNum: Int = consoleSinkParam.rowNum
    if (rowNum <= 20) {
      dataFrame.show(100)
    } else {
      sqlContext.sql(s"select * from ${consoleSinkParam.tableName}").show(rowNum)
    }
  }
}
