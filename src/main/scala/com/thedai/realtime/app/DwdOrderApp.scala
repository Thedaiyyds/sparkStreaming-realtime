package com.thedai.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONObject}
import com.thedai.realtime.bean.{OrderDetail, OrderInfo, OrderWide}
import com.thedai.realtime.util.{MyEsUtils, MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.TopicPartition
import org.apache.spark.SparkConf
import org.apache.spark.sql.catalyst.plans.FullOuter
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import redis.clients.jedis.Jedis

import java.time.{LocalDate, Period}
import java.util
import scala.collection.mutable.ListBuffer


object DwdOrderApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("dwd_order_app").setMaster("local[4]")
    val sc = new StreamingContext(conf, Seconds(5))

    //读取offset
    //order_info
    val orderInfoTopicName = "DWD_ORDER_INFO_I"
    val orderInfoGroup = "DWD_ORDER_INFO_GROUP"
    val orderInfoOffsets:Map[TopicPartition,Long] = MyOffsetUtils.readOffset(orderInfoTopicName,orderInfoGroup)
    //order_detail
    val orderDetailTopicName = "DWD_ORDER_DETAIL_I"
    val orderDetailGroup = "DWD_ORDER_DETAIL_GROUP"
    val orderDetailOffsets: Map[TopicPartition, Long] = MyOffsetUtils.readOffset(orderDetailTopicName, orderDetailGroup)

    //从kafka中消费数据
    //order_info
    var orderInfoKafkaDStream:InputDStream[ConsumerRecord[String,String]] = null
    if(orderInfoOffsets != null && orderInfoOffsets.nonEmpty){
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(sc,orderInfoTopicName,orderInfoGroup,orderInfoOffsets)
    }else{
      orderInfoKafkaDStream = MyKafkaUtils.getKafkaDStream(sc,orderInfoTopicName,orderInfoGroup)
    }
    //order_detail
    var orderDetailKafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (orderDetailOffsets != null && orderDetailOffsets.nonEmpty) {
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(sc, orderDetailTopicName, orderDetailGroup, orderDetailOffsets)
    } else {
      orderDetailKafkaDStream = MyKafkaUtils.getKafkaDStream(sc, orderDetailTopicName, orderDetailGroup)
    }

    //提取offset
    //order_info
    var orderInfoOffsetRanges:Array[OffsetRange] = null
    val orderInfoOffsetDStream: DStream[ConsumerRecord[String, String]] = orderInfoKafkaDStream.transform(
      rdd => {
        orderInfoOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )
    //order_detail
    var orderDetailOffsetRanges:Array[OffsetRange] = null
    val orderDetailOffsetDStream: DStream[ConsumerRecord[String, String]] = orderDetailKafkaDStream.transform(
      rdd => {
        orderDetailOffsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    //提取value
    //order_info
    val orderInfoDStream = orderInfoOffsetDStream.map(
      consumerRecord => {
        val value = consumerRecord.value()
        val orderInfo: OrderInfo = JSON.parseObject(value, classOf[OrderInfo])
        orderInfo
      }
    )

    //order_detail
    val orderDetailDStream = orderDetailOffsetDStream.map(
      consumerRecord => {
        val value = consumerRecord.value()
        val orderDetail: OrderDetail = JSON.parseObject(value, classOf[OrderDetail])
        orderDetail
      }
    )

    //维度关联
    //order_info
    val orderInfoDimDStream: DStream[OrderInfo] = orderInfoDStream.mapPartitions(
      orderInfoIter => {
        val orderInfoList: List[OrderInfo] = orderInfoIter.toList
        val jedis: Jedis = MyRedisUtils.getJedis()
        for (orderInfo <- orderInfoList) {
          //关联用户维度
          val uid: Long = orderInfo.user_id
          val redisUserKey = s"DIM:USER_INFO:$uid"
          val userInfoJson: String = jedis.get(redisUserKey)
          val userInfoJsonObj: JSONObject = JSON.parseObject(userInfoJson)
          //提取性别
          val gender: String = userInfoJsonObj.getString("gender")
          //提取生日
          val birthday: String = userInfoJsonObj.getString("birthday")
          //换算年龄
          val birtdayLd: LocalDate = LocalDate.parse(birthday)
          val nowLd: LocalDate = LocalDate.now()
          val period: Period = Period.between(birtdayLd, nowLd)
          val age: Int = period.getYears

          //补充到对象中
          orderInfo.user_age = age
          orderInfo.user_gender = gender

          //关联地区维度
          val provinceID: Long = orderInfo.province_id
          val redisProvinceKey: String = s"DIM:BASE_PROVINCE:$provinceID"
          val provinceJson: String = jedis.get(redisProvinceKey)
          val provinceJsonObj: JSONObject = JSON.parseObject(provinceJson)

          val provinceName: String = provinceJsonObj.getString("name")
          val provinceAreaCode: String = provinceJsonObj.getString("area_code")
          val province3166: String = provinceJsonObj.getString("iso_3166_2")
          val provinceIsoCode: String = provinceJsonObj.getString("iso_code")

          //补充到对象中
          orderInfo.province_name = provinceName
          orderInfo.province_area_code = provinceAreaCode
          orderInfo.province_3166_2_code = province3166
          orderInfo.province_iso_code = provinceIsoCode

          //处理日期字段
          val createTime: String = orderInfo.create_time
          val createDate: Array[String] = createTime.split(" ")
          val createDT: String = createDate(0)
          val createHr: String = createDate(1).split(":")(0)

          orderInfo.create_date = createDT
          orderInfo.create_hour = createHr

        }
        jedis.close()
        orderInfoList.toIterator
      }
    )

    //双流连接join
    //想要进行join操作之前需要把数据转换为二元组根据key去join
    //order_info
    val orderInfoKVDStream: DStream[(Long, OrderInfo)] = orderInfoDimDStream.map(
      orderInfo => (
        (orderInfo.id, orderInfo)
        )
    )
    val orderDetailKVDStream: DStream[(Long, OrderDetail)] = orderDetailDStream.map(
      orderDetail => (
        (orderDetail.order_id, orderDetail)
        )
    )
    //val orderJoinDStream: DStream[(Long, (OrderInfo, OrderDetail))] = orderInfoKVDStream join orderDetailKVDStream

    //orderJoinDStream.print(1000)

    //因为可能会出现数据延迟导致两个流应该在同一批次的数据却没有，导致数据丢失
    //所以使用全外连接先将数据拿到
    val orderFullJoinDStream: DStream[(Long, (Option[OrderInfo], Option[OrderDetail]))] = orderInfoKVDStream fullOuterJoin orderDetailKVDStream
    //对数据机进行拼接，并通过redis存储迟到数据进行拼接
    val redisJoinCompletedDStream: DStream[OrderWide] = orderFullJoinDStream.mapPartitions(
      orderJoinIter => {
        val orderWides: ListBuffer[OrderWide] = ListBuffer[OrderWide]()
        val jedis: Jedis = MyRedisUtils.getJedis()
        for ((key, (orderInfoOp, orderDetailOp)) <- orderJoinIter) {
          //三种情况
          //orderInfo有 orderDetail没有
          if (orderInfoOp.isDefined) {
            //取出orderInfo
            val orderInfo = orderInfoOp.get
            if (orderDetailOp.isDefined) {
              //取出orderDetail
              val orderDetail = orderDetailOp.get
              //组装成orderWide
              val orderWide: OrderWide = new OrderWide(orderInfo, orderDetail)
              orderWides.append(orderWide)
            }

            /**
             * 因为一个orderInfo和多个orderDetail关联所以orderInfo每次都应该写缓存
             * orderInfo写缓存
             * 类型:String
             * key:ORDERJOIN:ORDER_INFO:ID
             * value:json
             * 写入API:set
             * 读取API:get
             * 是否过期:24小时
             */
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${orderInfo.id}"
            jedis.setex(redisOrderInfoKey, 24 * 3600, JSON.toJSONString(orderInfo, new SerializeConfig(true)))
            //orderInfo有，orderDetail没有

            //orderInfo读缓存，去看看有没有orderDetail在等他
            val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderInfo.id}"
            val orderDetails: util.Set[String] = jedis.smembers(redisOrderDetailKey)
            if (orderDetails != null && orderDetails.size > 0) {
              import scala.collection.JavaConverters._
              for (orderDetailJson <- orderDetails.asScala) {
                val orderRedisDetail: OrderDetail = JSON.parseObject(orderDetailJson, classOf[OrderDetail])
                //组装成orderWide
                val orderRedisWide: OrderWide = new OrderWide(orderInfo, orderRedisDetail)
                //加入到结果集
                orderWides.append(orderRedisWide)
              }
            }

          } else {
            //orderInfo没有 orderDetail有
            val orderDetail: OrderDetail = orderDetailOp.get
            //读缓存
            val redisOrderInfoKey: String = s"ORDERJOIN:ORDER_INFO:${orderDetail.order_id}"
            val orderInfoJson: String = jedis.get(redisOrderInfoKey)
            if (orderInfoJson != null && orderInfoJson.nonEmpty) {
              val orderInfo = JSON.parseObject(orderInfoJson, classOf[OrderInfo])
              //组装成orderWide
              val orderWide = new OrderWide(orderInfo, orderDetail)
              orderWides.append(orderWide)
            } else {
              //因为orderDetail只能和一个orderInfo对应，所以找到不用写缓存，找不到就要写
              /**
               * orderDetail写缓存
               * 类型:set
               * key:ORDERJOIN:ORDER_DETAIL:ORDER_ID
               * value:json,json
               * 写入API:sadd
               * 读取API:smembers
               * 是否过期:24小时
               */
              val redisOrderDetailKey: String = s"ORDERJOIN:ORDER_DETAIL:${orderDetail.order_id}"
              jedis.sadd(redisOrderDetailKey, JSON.toJSONString(orderDetail, new SerializeConfig(true)))
              jedis.expire(redisOrderDetailKey, 24 * 3600)
            }

          }
        }
        jedis.close()
        orderWides.toIterator
      }
    )

    //写入ES
    //创建索引模板 gmall_order_wide_template
    redisJoinCompletedDStream.foreachRDD(
      rdd =>{
        rdd.foreachPartition(
          orderWideIter =>{
            val orderWides: List[(String, OrderWide)] = orderWideIter.map(orderWide => (orderWide.detail_id.toString, orderWide)).toList
            if (orderWides.nonEmpty){
              val head: (String, OrderWide) = orderWides.head
              val date: String = head._2.create_date
              val indexName: String = s"gmall_order_wide_$date"
              //写入ES
              MyEsUtils.bulkSave(indexName,orderWides)
            }
          }
        )
        MyOffsetUtils.saveOffset(orderInfoTopicName,orderInfoGroup,orderInfoOffsetRanges)
        MyOffsetUtils.saveOffset(orderDetailTopicName,orderDetailGroup,orderDetailOffsetRanges)
      }
    )
    sc.start
    sc.awaitTermination()

  }
}
