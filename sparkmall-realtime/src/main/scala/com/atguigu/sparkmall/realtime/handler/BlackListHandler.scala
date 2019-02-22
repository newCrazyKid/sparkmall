package com.atguigu.sparkmall.realtime.handler

import java.util
import java.util.Properties

import com.atguigu.sparkmall.common.util.PropertiesUtil
import com.atguigu.sparkmall.realtime.bean.AdsLog
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object BlackListHandler {

    def handle(adsLogDstream: DStream[AdsLog])={
        //每日每用户每个广告的点击数
        val clickcountPerDayUserAdDStream: DStream[(String, Long)] = adsLogDstream.map { adsLog => (adsLog.getDate() + "_" + adsLog.userId + "_" + adsLog.adsId, 1L) }
          .reduceByKey(_ + _)

        //保存到redis，结构：key:时间 : hash[field:每用户每广告, value:点击数]
        clickcountPerDayUserAdDStream.foreachRDD{rdd =>
            val properties: Properties = PropertiesUtil.load("config.properties")

            rdd.foreachPartition{adsIter =>
                //建立redis连接，写在foreachPartition里，每个分区都可以创建和使用到
                val jedis = new Jedis(properties.getProperty("redis.host"), properties.getProperty("redis.port").toInt)
                
                adsIter.foreach{case (logkey, count) =>
                    val day_user_ads: Array[String] = logkey.split("_")
                    val day: String = day_user_ads(0)
                    val user: String = day_user_ads(1)
                    val ads: String = day_user_ads(2)
                    val key = "user_ads_click:" + day

                    //累加
                    jedis.hincrBy(key, user+"_"+ads, count)
                    //判断点击数否达到了100，达到100则加入黑名单
                    val curCount: String = jedis.hget(key, user+"_"+ads)
                    if(curCount.toLong >= 100){
                        jedis.sadd("blacklist", user)
                    }
                }

                jedis.close()

            }
        }

    }

    def check(sparkContext: SparkContext, adsLogDstream: DStream[AdsLog])={
        //根据blacklist过滤，使用transform会每隔一段时间执行里面的代码
        val filterAdsLogDstream: DStream[AdsLog] = adsLogDstream.transform { rdd =>
            val properties: Properties = PropertiesUtil.load("config.properties")
            val jedis = new Jedis(properties.getProperty("redis.host"), properties.getProperty("redis.port").toInt)
            val blackSet: util.Set[String] = jedis.smembers("blacklist")
            //Driver中执行
            val blackBC: Broadcast[util.Set[String]] = sparkContext.broadcast(blackSet)
            rdd.filter { adsLog =>
                println(blackBC.value)
                !blackBC.value.contains(adsLog.userId) //Executor中执行
            }
        }
        filterAdsLogDstream
    }
}
