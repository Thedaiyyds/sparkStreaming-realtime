package com.thedai.realtime.util

import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.kafka010.OffsetRange

import java.util
import scala.collection.mutable

/**
 * offset管理工具类，用于往redis中存储或提取offset
 * 1.从kafka中消费数据，先提取偏移量
 * 2.等数据成功写出后将偏移量存储到redis
 *
 * 手动提交offset
 */
object MyOffsetUtils {

  /**
   * 往redis中存储offset
   * kafka存储offset结构
   * key groupid + topic + partition
   * value offset的值
   *
   * 存储结构：Hash
   * key：groupid + topic
   * value： partition - offset
   */
  def saveOffset(topic:String,groupId:String,offsetRanges:Array[OffsetRange]): Unit = {
    if (offsetRanges != null && offsetRanges.size > 0){
      val offsets: java.util.HashMap[String,String] = new util.HashMap();
      for (offsetRange <- offsetRanges){
        val partition = offsetRange.partition
        val endOffset = offsetRange.untilOffset
        offsets.put(partition.toString,endOffset.toString)
      }
      println("提交offset"+offsets)
      val jedis = MyRedisUtils.getJedis()
      jedis.hset(s"offset$topic$groupId",offsets)
      jedis.close
    }
  }

  /**
   * 往redis读取offset
   *
   * sparkStreaming指定offset，传入offset
   * 类型：Map[TopicPartition, Long]
   */
  def readOffset(topic:String,groupId:String):Map[TopicPartition, Long]={
    val jedis = MyRedisUtils.getJedis()
    val offsets = jedis.hgetAll(s"offset$topic$groupId")
    val map = mutable.Map[TopicPartition, Long]()
    println("读取到offset"+map)
    import scala.collection.JavaConverters._
    for((partititon,offset) <- offsets.asScala){
      val tp = new TopicPartition(topic, partititon.toInt)
      map.put(tp,offset.toLong)
    }
    jedis.close()
    map.toMap
  }
}
