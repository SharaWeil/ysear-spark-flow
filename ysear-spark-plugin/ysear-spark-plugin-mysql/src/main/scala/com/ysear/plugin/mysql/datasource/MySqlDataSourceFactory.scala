package com.ysear.plugin.mysql.datasource

import com.ysear.spark.spi.datasource.{BaseDataSource, FlowDataSourceFactory}
import org.dom4j.Element

/*
 * @Author Administrator
 * @Date 2023/1/6
 **/
class MySqlDataSourceFactory extends FlowDataSourceFactory{
  /**
   * 工厂类型
   *
   * @return
   */
  override def getType: String = "Mysql"

  override def create(element: Element): BaseDataSource = {
    val sourceType: String = element.attributeValue("type")
    val sourceId: String = element.attributeValue("sourceId")
    val driverClass: String = element.element("driverClass").getText
    val url: String = element.element("url").getText
    val userName: String = element.element("userName").getText
    val passWord: String = element.element("passWord").getText
    new MysqlDataSource(sourceType,sourceId,driverClass,url,userName,passWord)
  }
}
