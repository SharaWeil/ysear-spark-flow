package com.ysear.spark.common.implicits

import org.apache.commons.lang3.StringUtils


/*
 * @Author Administrator
 * @Date 2022/12/27
 **/
object TypeConvert {
    implicit def toInt(v:String):Int={
      if(StringUtils.isNotBlank(v)){
        Integer.parseInt(v)
      }
      1
    }
}
