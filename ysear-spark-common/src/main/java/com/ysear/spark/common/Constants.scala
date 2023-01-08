package com.ysear.spark.common

/**
 * @ClassName Constants.scala
 * @createTime 2022年12月27日 14:39:00
 */
object Constants {

  val DATA_SOURCE_XPATH_EXPRESSION:String = "/flow/dataSources/dataSource"

  val SOURCE_NODE_XPATH_EXPRESSION:String = "/flow/sources/source"

  val FLOW_INFO_XPATH_EXPRESSION:String = "/flow/info"

  val UDF_XPATH_EXPRESSION:String = "/flow/methods/udf"

  val TRANSFORM_NODE_XPATH_EXPRESSION:String = "/flow/transforms/transform"

  val SINK_NODE_XPATH_EXPRESSION:String = "/flow/targets/target"

  val CONFIG_XPATH_EXPRESSION:String = "/flow/properties/sparkConfs"
}
