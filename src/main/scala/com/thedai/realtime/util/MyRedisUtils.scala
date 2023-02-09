package com.thedai.realtime.util

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}
import java.time.Duration

//redis
object MyRedisUtils {
  var jedisPool:JedisPool = _

  def getJedis(): Jedis = {
    if (jedisPool == null) {
      val host = MyPropertiesUtils("redis.host")
      val port = MyPropertiesUtils("redis.port").toInt
      val user = MyPropertiesUtils("redis.user")
      val password = MyPropertiesUtils("redis.password")

      val jedisPoolConfig = new JedisPoolConfig()
      jedisPoolConfig.setMaxTotal(100) //最大连接数
      jedisPoolConfig.setMaxIdle(20) //最大空闲
      jedisPoolConfig.setMinIdle(20) //最小空闲
      jedisPoolConfig.setBlockWhenExhausted(true) //忙碌时是否等待
      jedisPoolConfig.setMaxWait(Duration.ofMillis(5000)) //忙碌时等待时长毫秒
      jedisPoolConfig.setTestOnBorrow(true) //每次获得连接的进行测试
      jedisPool = new JedisPool(jedisPoolConfig, host, port,user,password)
    }
    jedisPool.getResource
  }
}
