package com.thedai.realtime.util

import java.lang.reflect.{Field, Method, Modifier}
import scala.util.control.Breaks

/**
 * 实现对象属性拷贝.
 */
object MyBeanUtils {

  /**
   * 将 srcObj 中属性的值拷贝到 destObj 对应的属性上.
   */
  def copyProperties(srcObj : AnyRef , destObj: AnyRef): Unit ={
    if(srcObj == null || destObj == null ){
      return
    }
    //获取到 srcObj 中所有的属性
    val srcFields: Array[Field] = srcObj.getClass.getDeclaredFields
    //处理每个属性的拷贝
    for (srcField <- srcFields) {
      Breaks.breakable{
        //get / set
        // Scala 会自动为类中的属性提供 get、 set 方法
        // get : fieldname()
        // set : fieldname_$eq(参数类型)
        //getMethodName
        var getMethodName : String = srcField.getName
        //setMethodName
        var setMethodName : String = srcField.getName+"_$eq"
        //从 srcObj 中获取 get 方法对象，
        val getMethod: Method = srcObj.getClass.getDeclaredMethod(getMethodName)
        //从 destObj 中获取 set 方法对象
        // String name;
        // getName()
        // setName(String name ){ this.name = name }
        val setMethod: Method =
        try{
          destObj.getClass.getDeclaredMethod(setMethodName, srcField.getType)
        }catch{
          // NoSuchMethodException
          case ex : Exception => Breaks.break()
        }
        //忽略 val 属性
        val destField: Field =
          destObj.getClass.getDeclaredField(srcField.getName)
        if(destField.getModifiers.equals(Modifier.FINAL)){
          Breaks.break()
        }
        //调用 get 方法获取到 srcObj 属性的值， 再调用 set 方法将获取到的属性 值赋值给 destObj 的属性
        setMethod.invoke(destObj, getMethod.invoke(srcObj))
      }
    }
  }
}
