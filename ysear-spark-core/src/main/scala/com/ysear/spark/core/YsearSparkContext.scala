package com.ysear.spark.core

import com.ysear.spark.spi.PluginManager
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/*
 * @Author Administrator
 * @Date 2023/1/7
 **/
class YsearSparkContext(sparkSql:SQLContext) {

  private val sparkContext: SparkContext = sparkSql.sparkContext


}
