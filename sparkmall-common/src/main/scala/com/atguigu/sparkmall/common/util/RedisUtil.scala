package com.atguigu.sparkmall.common.util

import java.util.Properties

import redis.clients.jedis.{JedisPool, JedisPoolConfig}

object RedisUtil {

    var jedisPool: JedisPool = null

    def getJedisClient() = {
        if(jedisPool == null){
            println("创建一个连接池")
            val properties: Properties = PropertiesUtil.load("config.properties")
            val host: String = properties.getProperty("redis.host")
            val port: Int = properties.getProperty("redis.port").toInt

            val jedisPoolConfig = new JedisPoolConfig
            jedisPoolConfig.setMaxTotal(100)//最大连接数
            jedisPoolConfig.setMaxIdle(20)//最大空闲
            jedisPoolConfig.setMinIdle(20)//最小空闲
            jedisPoolConfig.setBlockWhenExhausted(true)//忙碌时是否等待
            jedisPoolConfig.setMaxWaitMillis(500)//忙碌时等待时长
            jedisPoolConfig.setTestOnBorrow(true)//每次获得连接是否先进行测试（某个连接可能已经被使用）

            jedisPool = new JedisPool(jedisPoolConfig, host, port)

        }

        println(s"jedisPool.getNumActive = ${jedisPool.getNumActive}")
        println("获得一个连接")
        jedisPool.getResource
    }
}
