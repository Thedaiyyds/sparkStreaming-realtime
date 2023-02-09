package com.thedai.realtime.app

import com.alibaba.fastjson.serializer.SerializeConfig
import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}
import com.thedai.realtime.bean.{PageActionLog, PageDisplayLog, PageLog, StartLog}
import com.thedai.realtime.util.{MyKafkaUtils, MyOffsetUtils, MyRedisUtils}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.SparkConf
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{HasOffsetRanges, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}


object OdsBaseLogApp {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().setAppName("ods_base_log_app").setMaster("local[4]")
    val sc = new StreamingContext(conf,Seconds(5))

    //ODS层
    //从kafka中消费数据
    val topicName:String = "ODS_BASE_LOG_Thedai"
    val groupId:String = "ODS_BASE_LOG_Group"

    val offsets = MyOffsetUtils.readOffset(topicName, groupId)
    var kafkaDStream: InputDStream[ConsumerRecord[String, String]] = null
    if (offsets != null && offsets.nonEmpty){
      kafkaDStream = MyKafkaUtils.getKafkaDStream(sc, topicName, groupId)
    }else{
      kafkaDStream = MyKafkaUtils.getKafkaDStream(sc, topicName, groupId,offsets)
    }
    //TODO 从消费数据中提取offsets，但是不对数据做任何处理
    var offsetRanges:Array[OffsetRange] = null
    val offsetRangesDStream: DStream[ConsumerRecord[String, String]] = kafkaDStream.transform(
      rdd => {
        offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
        rdd
      }
    )

    val ODSDStream = offsetRangesDStream.map(
      consumerRecord => {
        //获取ConsumerRecord中的value值
        val log: String = consumerRecord.value()
        //转换成Json对象
        val jsonObj: JSONObject = JSON.parseObject(log)
        //返回
        jsonObj
      }
    )


    //DWD层
    //页面访问
    val DWD_PAGE_LOG_TOPIC : String = "DWD_PAGE_LOG_TOPIC"
    //页面曝光
    val DWD_PAGE_DISPLAY_TOPIC : String = "DWD_PAGE_DISPLAY_TOPIC"
    //页面事件
    val DWD_PAGE_ACTION_TOPIC : String = "DWD_PAGE_ACTION_TOPIC"
    //启动数据
    val DWD_START_LOG_TOPIC : String = "DWD_START_LOG_TOPIC"
    //错误数据
    val DWD_ERROR_LOG_TOPIC : String = "DWD_ERROR_LOG_TOPIC"

    //分流规则：
    // 错误数据：不做任何拆分，包含错误字段，直接将整条数据发送至相应的topic
    // 页面数据：拆分成页面访问，曝光，事件 分别发送到相应的topic
    // 启动数据：发送到对应的topic
    ODSDStream.foreachRDD(
      rdd =>{
        rdd.foreachPartition(
          jsonObjIter =>{
          for(jsonObj <- jsonObjIter){
            val errJSONObject = jsonObj.getJSONObject("err")
            if (errJSONObject != null) {
              MyKafkaUtils.send(DWD_ERROR_LOG_TOPIC, errJSONObject.toJSONString)
            } else {
              //提取公共数据
              val common = jsonObj.getJSONObject("common")
              val ar = common.getString("ar")
              val uid = common.getString("uid")
              val os = common.getString("os")
              val ch = common.getString("ch")
              val is_new = common.getString("is_new")
              val md = common.getString("md")
              val mid = common.getString("mid")
              val vc = common.getString("vc")
              val ba = common.getString("ba")
              //提取时间戳
              val ts = jsonObj.getLong("ts")
              //页面数据
              val pageObj = jsonObj.getJSONObject("page")
              if (pageObj != null) {
                val pageId: String = pageObj.getString("page_id")
                val pageItem: String = pageObj.getString("item")
                val pageItemType: String = pageObj.getString("item_type")
                val duringTime: Long = pageObj.getLong("during_time")
                val lastPageId: String = pageObj.getString("last_page_id")
                val sourceType: String = pageObj.getString("source_type")
                //发送到DWD_PAGE_LOG_TOPIC
                val pageLog: PageLog = PageLog(mid, uid, ar, ch, is_new, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, ts)
                MyKafkaUtils.send(DWD_PAGE_LOG_TOPIC, JSON.toJSONString(pageLog, new SerializeConfig(true)))

                //提取曝光数据
                val displaysArrayObj: JSONArray = jsonObj.getJSONArray("displays")
                if (displaysArrayObj != null && displaysArrayObj.size() > 0) {
                  for (i <- 0 until displaysArrayObj.size()) {
                    //循环拿到每一个曝光
                    val displayObj: JSONObject = displaysArrayObj.getJSONObject(i)
                    //提取曝光字段
                    val displayType: String = displayObj.getString("display_type")
                    val displayItem: String = displayObj.getString("item")
                    val displayItemType: String = displayObj.getString("item_type")
                    val posId: String = displayObj.getString("pos_id")
                    val order: String = displayObj.getString("order")
                    //写出到DWD_PAGE_DISPLAY_TOPIC
                    val displayLog = PageDisplayLog(mid, uid, ar, ch, is_new, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, displayType, displayItem, displayItemType, order, posId, ts)
                    MyKafkaUtils.send(DWD_PAGE_DISPLAY_TOPIC, JSON.toJSONString(displayLog, new SerializeConfig(true)))
                  }
                }
                //提取事件数据
                val actionJsonArr: JSONArray = jsonObj.getJSONArray("actions")
                if (actionJsonArr != null && actionJsonArr.size() > 0) {
                  for (i <- 0 until actionJsonArr.size()) {
                    //循环拿到每一个事件
                    val actionObj: JSONObject = actionJsonArr.getJSONObject(i)
                    //提取事件字段
                    val actionId = actionObj.getString("action_id")
                    val actionItem = actionObj.getString("item")
                    val actionItemType = actionObj.getString("item_type")
                    val actionTs = actionObj.getLong("ts")
                    //写出到DWD_PAGE_ACTION_TOPIC
                    val pageActionLog: PageActionLog = PageActionLog(mid, uid, ar, ch, is_new, md, os, vc, ba, pageId, lastPageId, pageItem, pageItemType, duringTime, sourceType, actionId, actionItem, actionItemType, actionTs, ts)
                    MyKafkaUtils.send(DWD_PAGE_ACTION_TOPIC, JSON.toJSONString(pageActionLog, new SerializeConfig(true)))
                  }
                }
              }
              //提取启动数据
              val startJsonObj: JSONObject = jsonObj.getJSONObject("start")
              if (startJsonObj != null) {
                //提取字段
                val entry: String = startJsonObj.getString("entry")
                val loadingTime: Long = startJsonObj.getLong("loading_time")
                val openAdId: String = startJsonObj.getString("open_ad_id")
                val openAdMs: Long = startJsonObj.getLong("open_ad_ms")
                val openAdSkipMs: Long = startJsonObj.getLong("open_ad_skip_ms")

                //发送数据到DWD_START_LOG_TOPIC
                val startLog: StartLog = StartLog(mid, uid, ar, ch, is_new, md, os, vc, entry, openAdId, loadingTime, openAdMs, openAdSkipMs, ts)
                MyKafkaUtils.send(DWD_START_LOG_TOPIC, JSON.toJSONString(startLog, new SerializeConfig(true)))
              }
            }
            }
          //每批次每分区执行一次 executor端 foreachPartition内
            MyKafkaUtils.flush
          }
        )
        //每批次执行一个 driver端 foreachPartition外，foreachRdd内
        MyOffsetUtils.saveOffset(topicName,groupId,offsetRanges)
      }
    )
    sc.start
    sc.awaitTermination()
  }
}
