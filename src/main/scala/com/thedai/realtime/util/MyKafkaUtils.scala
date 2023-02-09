package com.thedai.realtime.util


import org.apache.kafka.clients.consumer.{ConsumerConfig, ConsumerRecord}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import scala.collection.immutable.Map
import scala.collection.mutable

//kafka生产者消费者
object MyKafkaUtils {
  private val consumerConfigs:mutable.Map[String,Object] = mutable.Map[String,Object](
    //kfaka集群位置
    ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> "hadoop101:9092,hadoop102:9092,hadoop103:9092",
    //kv反序列化器
    ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> "org.apache.kafka.common.serialization.StringDeserializer",
    //groupid
    //offset提交策略 自动手动 默认是自动提交
    ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG -> "true",
    //自动提交间隔时间 默认是5000ms
    //ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG
    //offset重置策略 默认是从尾开始消费 earliest latest
    ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "latest"
  )

  /**
   * 基于sparkStreaming消费，获取到KafkaDStream
   * @param sc
   * @param topic
   * @param groupId
   * @return
   */
  def getKafkaDStream(sc:StreamingContext,topic:String,groupId:String): InputDStream[ConsumerRecord[String, String]] ={
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG,groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      sc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topic), consumerConfigs))
    kafkaDStream
  }

  /**
   *
   * @param sc
   * @param topic
   * @param groupId
   * @param offsets
   * @return
   */
  def getKafkaDStream(sc: StreamingContext, topic: String, groupId: String,offsets: Map[TopicPartition, Long]): InputDStream[ConsumerRecord[String, String]] = {
    consumerConfigs.put(ConsumerConfig.GROUP_ID_CONFIG, groupId)
    val kafkaDStream: InputDStream[ConsumerRecord[String, String]] = KafkaUtils.createDirectStream(
      sc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topic), consumerConfigs,offsets))
    kafkaDStream
  }
  val producer : KafkaProducer[String,String] = createProducer()

  def createProducer():KafkaProducer[String,String] ={
    val producerConfigs : java.util.HashMap[String,AnyRef] = new java.util.HashMap[String,AnyRef]
    //kafka集群位置
    producerConfigs.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"hadoop101:9092,hadoop102:9092,hadoop103:9092")
    //kv序列化器
    producerConfigs.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    producerConfigs.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,"org.apache.kafka.common.serialization.StringSerializer")
    //acks应答级别 0：不需要等待应答 1：等待leader应答 -1/all：等待leader的所有follower应答
    producerConfigs.put(ProducerConfig.ACKS_CONFIG,"-1")
    //batch.size 默认数据积累到16kb
    //linger.ms 等待一定时间发送一批
    //retries 重试次数
    //幂等配置 默认false
    producerConfigs.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG,"true")
    ProducerConfig.RETRIES_CONFIG
    val kafkaProducer: KafkaProducer[String, String] = new KafkaProducer[String, String](producerConfigs)
    kafkaProducer
  }
  //按照默认的粘性分区策略(会尽量的将数据打乱到各个分区)发送数据
  def send(topic:String,msg:String){
    producer.send(new ProducerRecord[String,String](topic,msg))
  }
  //按照key进行分区发送数据
  def send(topic:String,key:String,msg:String){
    producer.send(new ProducerRecord[String,String](topic,key,msg))
  }
  def flush(): Unit = {
    producer.flush()
  }
  def close(){
    if (producer != null) producer.close()
  }
}
