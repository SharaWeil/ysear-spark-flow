package com.ysear.plugin.mysql.flowsource

import com.ysear.plugin.datasource.jdbc.JDBCFlowSource
import com.ysear.spark.spi.source.FlowSourceParam
import org.apache.spark.sql.DataFrame


/*
 * @Author Administrator
 * @Date 2023/1/7
 **/
class MySqlFlowSource(flowSourceParam: FlowSourceParam) extends JDBCFlowSource(flowSourceParam){

  override def createDataFrame: DataFrame = {
    super.createDataFrame
  }
}
