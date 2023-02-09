package com.thedai.realtime.util

import java.util.ResourceBundle

//配置工具类
object MyPropertiesUtils {

  private val resourceBundle = ResourceBundle.getBundle("config")

  def apply(name:String)={
    resourceBundle.getString(name)
  }
}
