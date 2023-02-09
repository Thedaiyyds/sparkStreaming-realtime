package com.thedai.realtime.app

import com.alibaba.fastjson.JSON
import com.thedai.realtime.bean.{DauInfo, PageLog}
import com.thedai.realtime.util.{MyBeanUtils, MyEsUtils, MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.{Jedis, Pipeline}

import java.text.SimpleDateFormat
import java.time.{LocalDate, Period}
import java.util.Date
import scala.collection.mutable.ListBuffer

object DwdDauApp {
  def main(args: Array[String]): Unit = {
    //还原状态
    revertState()

    val conf = new SparkConf().setAppName("dwd_dau_app").setMaster("local[4]")
    val sc: StreamingContext = new StreamingContext(conf, Seconds(5))

    val topic : String = "DWD_PAGE_LOG_TOPIC"
    val groupId : String = "DWD_DAU_GROUP"
    //从redis中获取offset
    val offsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(topic, groupId)

    //从kafka中消费数据
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null;

    if (offsets != null && offsets.nonEmpty) {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(sc, topic, groupId, offsets)
    } else {
      kafkaDStream = MyKafkaUtils.getKafkaDStream(sc, topic, groupId)
    }
    //提取offset结束点
    var offsetRanges: Array[OffsetRange] = null;
    val offsetDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    val pageLogDStream: DStream[PageLog] = offsetDStream.map(
      consumerRecord => {
        val value: String = consumerRecord.value()
        val pageLog = JSON.parseObject(value, classOf[PageLog])
        pageLog
      }
    )

    //对页面访问数据进行去重，将last_page_id不为空的数据过滤(只保留用户第一次访问页面的记录)
    val filterDStream: DStream[PageLog] = pageLogDStream.filter(
      pageLog => pageLog.last_page_id == null
    )

    /**
     * 在redis进行审查，redis维护了当日活跃的mid，然后获取redis中的mid
     * redis维护日活
     * 类型：set
     * key：DAU:DATE(因为维护的是日活)
     * value：mid
     * 写入API：sadd
     * 读取API：smembers
     * 过期：
     */
    val redisFilterDStream: DStream[PageLog] = filterDStream.mapPartitions(
      pageLogIter => {
        val list= pageLogIter.toList
        println("去重前："+list.size)
        //存储需要的数据
        val pageLogs: ListBuffer[PageLog] = ListBuffer[PageLog]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
        val jedis: Jedis = MyRedisUtils.getJedis()
        for (pageLog <- list) {
          //提取每一条数据中的mid
          val mid: String = pageLog.mid
          //获取日期
          val ts: Long = pageLog.ts
          val data: Date = new Date(ts)
          val dataStr: String = sdf.format(data)
          val redisDauKey: String = s"DAU:$dataStr"

          val isNew = jedis.sadd(redisDauKey, mid)
          if (isNew == 1L) {
            pageLogs.append(pageLog)
          }
        }
        jedis.close()
        println("去重后："+pageLogs.size)
        pageLogs.iterator
      }
    )
    val dauInfoDStream: DStream[DauInfo] = redisFilterDStream.mapPartitions(
      pageLogIter => {
        val dauInfos: ListBuffer[DauInfo] = ListBuffer[DauInfo]()
        val sdf: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val jedis: Jedis = MyRedisUtils.getJedis()
        for (pageLog <- pageLogIter) {
          //将Page的数据全部拷贝到DauInfo中，使用BeauUtil快速拷贝
          val dauInfo: DauInfo = new DauInfo()
          MyBeanUtils.copyProperties(pageLog, dauInfo)
          //补充维度 调用Jedis DIM层中的维度表
          //用户信息维度
          val uid: String = pageLog.user_id
          val redisUidKey: String = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUidKey)
          val jSONObject = JSON.parseObject(userInfoJson)
          //提取性别
          val gender: String = jSONObject.getString("gender")
          //提取生日
          val birthday: String = jSONObject.getString("birthday")
          //根据生日换算年龄
          val birthdayLD: LocalDate = LocalDate.parse(birthday)
          val nowLD: LocalDate = LocalDate.now()
          val period: Period = Period.between(birthdayLD, nowLD)
          val age: Int = period.getYears
          //补充到对象中
          dauInfo.user_gender = gender
          dauInfo.user_age = age.toString
          //地区信息维度
          val provinceId = dauInfo.province_id
          val redisProvinceKey = s"DIM:BASE_PROVINCE:$provinceId"
          val provinceJson = jedis.get(redisProvinceKey)
          val provinceJsonObj = JSON.parseObject(provinceJson)
          val provinceName = provinceJsonObj.getString("name")
          val provinceIsoCode = provinceJsonObj.getString("iso_code")
          val province3166 = provinceJsonObj.getString("iso_3166_2")
          val provinceAreaCode = provinceJsonObj.getString("area_code")
          //日期字段处理
          val date: Any = new Date(pageLog.ts)
          val dateFormat = sdf.format(date)
          val dataFormatArr = dateFormat.split(" ")
          val dt: String = dataFormatArr(0)
          val hr = dataFormatArr(1).split(":")(0)
          dauInfo.dt = dt
          dauInfo.hr = hr

          dauInfos.append(dauInfo)
        }
        jedis.close()
        dauInfos.iterator
      }
    )

    //写入到ES
    //es中有相应的索引模板 gmall_dau_info_template
    dauInfoDStream.foreachRDD(
      rdd =>{
        rdd.foreachPartition(
          dauInfoIter => {
            val docs: List[(String, DauInfo)] = dauInfoIter.map(dauInfo => (dauInfo.mid, dauInfo)).toList
            if (docs.nonEmpty ){
              val head: (String, DauInfo) = docs.head
              val ts:Long = head._2.ts
              val sdf:SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
              val dateStr:String = sdf.format(new Date(ts))
              val indexName:String = s"gmall_dau_info_$dateStr"
              MyEsUtils.bulkSave(indexName,docs)
            }
          }
        )
        MyOffsetUtils.saveOffset(topic,groupId,offsetRanges)
      }
    )

    sc.start
    sc.awaitTermination()
  }


  /**
   * 为了防止mid写入了redis，但是没有写入es导致数据会丢失
   * 解决方案：恢复状态
   */
  def revertState(): Unit = {
    //从ES查询所有mid
    val now: LocalDate = LocalDate.now
    val indexName: String = s"gmall_dau_info_$now"
    val fieldName: String = "mid"
    val mids: List[String] = MyEsUtils.searchField(indexName, fieldName)

    //删除redis中记录的状态(所有mid)
    val jedis: Jedis = MyRedisUtils.getJedis()
    val redisDauKey: String = s"DAU:$now"
    jedis.del(redisDauKey)
    //将从ES查询到的mid覆盖到redis中
    if (mids != null && mids.nonEmpty) {
      //使用管道pipeline将所有mid进行初始化操作，
      val pipeline: Pipeline = jedis.pipelined()
      for (mid <- mids) {
        pipeline.sadd(redisDauKey, mid)
      }
      pipeline.sync()
    }
    jedis.close()
  }
}
