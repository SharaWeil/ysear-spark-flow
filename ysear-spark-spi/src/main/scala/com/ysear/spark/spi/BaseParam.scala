package com.ysear.spark.spi

/*
 * @Author Administrator
 * @Date 2023/1/6
 * dataSource基础类
 **/
class BaseParam extends Serializable {
  private var _strType: String = _

  private var _id: String = _

  def this(`type`:String,id:String){
    this()
    this.strType = `type`
    this._id = id
  }

  def strType: String = _strType

  def strType_=(value: String): Unit = {
    _strType = value
  }

  def id: String = _id

  def id_=(value: String): Unit = {
    _id = value
  }

}
