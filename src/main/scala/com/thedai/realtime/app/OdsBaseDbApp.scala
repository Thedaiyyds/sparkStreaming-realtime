package com.thedai.realtime.app

import com.alibaba.fastjson.{JSON, JSONObject}
import com.thedai.realtime.util.{MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.util

object OdsBaseDbApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ods_base_db_app").setMaster("local[4]")
    val sc = new StreamingContext(conf, Seconds(5))

    val topic:String = "ODS_BASE_DB"
    val groupId:String = "ODS_BASE_DB_GROUP"

    //从redis中读取偏移量
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topic, groupId)

    var kafkaDStream:InputDStream[ConsumerRecord[String, String]] = null;
    //从kafka中消费数据
    if (offsets != null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtils.getKafkaDStream(sc, topic, groupId,offsets)
    }else{
      kafkaDStream = MyKafkaUtils.getKafkaDStream(sc, topic, groupId)
    }
    //提取出offset
    var offsetRanges:Array[OffsetRange] = null;
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    val jsonDStream: DStream[JSONObject] = offsetDStream.map(
      consumerRecord => {
        val json = consumerRecord.value()
        val jSONObject = JSON.parseObject(json)
        jSONObject
      }
    )
    //jsonDStream.print(100)


    jsonDStream.foreachRDD(
      rdd =>{
        /**
         * 动态配置表清单
         * 将表清单存储在redis当中
         * 数据结构：set
         * key: FACT:TABLES DIM:TABLES
         * VALUE:表名的集合
         * 写入API:sadd
         * 读取API:smembers
         * 过期：不过期
         */

        val factKeys = "FACT:TABLES"
        val dimKeys = "DIM:TABLES"
        val jds = MyRedisUtils.getJedis()
        //set集合在driver端，后面使用的时候是在executor端使用所以需要进行网络传输，但是会在每个task中保存一份导致冗余
        //使用广播变量让数据在Executor只保存一份
        //事实表清单
        val factTables: java.util.Set[String] = jds.smembers(factKeys)
        val factTablesBC = sc.sparkContext.broadcast[util.Set[String]](factTables)
        //println("事实表"+factTables)
        //维度表清单
        val dimTables: java.util.Set[String] = jds.smembers(dimKeys)
        val dimTablesBC = sc.sparkContext.broadcast[util.Set[String]](dimTables)
        //println("维度表"+dimTables)
        rdd.foreachPartition(
          jsonObjIter =>{
            //为每批次每个分区的RDD创建一个jedis对象 excutor端
            val jedis: Jedis = MyRedisUtils.getJedis()
            for (jsonObj <- jsonObjIter){
              //提取操作类型
              val opType: String = jsonObj.getString("type")
              val op: String = opType match {
                case "bootstrap-insert" => "nothing"
                case "insert" => "I"
                case "update" => "U"
                case "delete" => "D"
                case _ => null
              }
              //判断操作类型
              if(op != null){
                //提取表明
                val tableName: String = jsonObj.getString("table")

                if(factTablesBC.value.contains(tableName)){
                  //事实数据
                  //提取数据
                  val data: String = jsonObj.getString("data")
                  val dwdTopicName : String = s"DWD_${tableName.toUpperCase}_$op"
                  MyKafkaUtils.send(dwdTopicName,data)
                }
                if(dimTablesBC.value.contains(tableName)){
                  /**
                   * 维度数据
                   * 存储到redis
                   * 数据类型：String（一条数据存储成一个JsonString）
                   * key：DIM:表名:ID
                   * value:整条数据的jsonString
                   * 写入API：set
                   * 读取API：get
                   * 过期：不过期
                   */

                  //提取数据
                  val dataJsonObj: JSONObject = jsonObj.getJSONObject("data")
                  val id = dataJsonObj.getString("id")
                  jedis.set(s"DIM:${tableName.toUpperCase}:$id",dataJsonObj.toJSONString)
                }
              }
            }
            //每批次每分区刷写一次
            jedis.close()
            MyKafkaUtils.flush()
          }
        )
        //每批次提交一次offset
        MyOffsetUtils.saveOffset(topic, groupId, offsetRanges)
      }
    )

    sc.start
    sc.awaitTermination()
  }
}
