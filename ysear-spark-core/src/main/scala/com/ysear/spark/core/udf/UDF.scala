package com.ysear.spark.core.udf

/**
 * @ClassName FlowUDF.scala
 * @createTime 2022年12月27日 09:43:00
 */
class UDF extends Serializable {

  var name: String = _

  var clazz: String = _

  var udfClass: Class[_] = _

  var returnType: String = _

  import org.apache.spark.sql.types.DataTypes

  import java.util.Objects

  def this(name: String, clazz: String, returnType: String) {
    this()
    this.name = name
    this.udfClass = Class.forName(clazz)
    this.clazz = clazz
    this.returnType = returnType
  }

  def getName: String = this.name

  def getUdfClass: Class[_] = this.udfClass

  def getUdfObject: Any =
    try {
      this.udfClass.newInstance
    }
    catch {
      case e: Exception =>
        throw new IllegalStateException(e)
    }

  def getSparkDataType: org.apache.spark.sql.types.DataType = getSparkDataType(this.returnType)

  def getSparkDataType(returnType: String): org.apache.spark.sql.types.DataType = {
    val containsFan: Boolean = returnType.contains("<") && returnType.contains(">")
    val `type`: String = if (containsFan) returnType.substring(0, returnType.indexOf("<")).toUpperCase
    else returnType.toUpperCase
    val fanTypes: Array[String] = if (containsFan) returnType.substring(returnType.indexOf("<") + 1, returnType.lastIndexOf(">")).split(",", -1)
    else new Array[String](0)
    `type` match {
      case "STRING" =>
        return DataTypes.StringType
      case "INT" =>
      case "INTEGER" =>
        return DataTypes.IntegerType
      case "LONG" =>
        return DataTypes.LongType
      case "FLOAT" =>
        return DataTypes.FloatType
      case "DOUBLE" =>
        return DataTypes.DoubleType
      case "SHORT" =>
        return DataTypes.ShortType
      case "BOOLEAN" =>
        return DataTypes.BooleanType
      case "DATE" =>
        return DataTypes.DateType
      case "TIMESTAMP" =>
        return DataTypes.TimestampType
      case "BYTE" =>
        return DataTypes.ByteType
      case "MAP" =>
        return DataTypes.createMapType(getSparkDataType(fanTypes(0).trim), getSparkDataType(fanTypes(1).trim)).asInstanceOf[Nothing]
      case "ARRAY" =>
        return DataTypes.createArrayType(getSparkDataType(fanTypes(0).trim)).asInstanceOf[Nothing]
    }
    throw new IllegalArgumentException("unknown udf return type: " + returnType)
  }


  def canEqual(other: Any): Boolean = other.isInstanceOf[UDF]

  override def equals(other: Any): Boolean = other match {
    case that: UDF =>
      if (that canEqual this) {
        return true
      }
      if (other == null || (getClass != other.getClass)) {
        return false
      }
      val udf: UDF = other.asInstanceOf[UDF]
      Objects.equals(this.name, udf.name)
    case _ => false
  }

  override def hashCode: Int = Objects.hash(Array[Object](this.name))
}
