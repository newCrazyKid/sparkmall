package com.atguigu.sparkmall.realtime.handler

import com.atguigu.sparkmall.common.util.RedisUtil
import com.atguigu.sparkmall.realtime.bean.AdsLog
import org.apache.spark.streaming.dstream.DStream
import redis.clients.jedis.Jedis

object AreaCityAdsCountHandler {

    def handle(adsLogDstream: DStream[AdsLog])={
        //整理dstream成kv结构（date:area:city:ads, 1L)
        val adsClickDstream: DStream[(String, Long)] = adsLogDstream.map { adsLog =>
            val key: String = adsLog.getDate() + ":" + adsLog.area + ":" + adsLog.city + ":" + adsLog.adsId
            (key, 1L)
        }

        //利用updateStateByKey进行累加
        val adsClickCountDstream: DStream[(String, Long)] = adsClickDstream.updateStateByKey { (countSeq: Seq[Long], total: Option[Long]) =>
            //把countSeq汇总，累加到total中
            println(countSeq.mkString(","))
            val countSum: Long = countSeq.sum
            val curTotal: Long = total.getOrElse(0L) + countSum
            Some(curTotal)
        }
        //保存到redis
        adsClickCountDstream.foreachRDD{rdd =>
            rdd.foreachPartition{adsClickCountIter =>
                val jedis: Jedis = RedisUtil.getJedisClient()
                adsClickCountIter.foreach{case(key, count) => jedis.hset("date:area:city:ads", key, count.toString)}
                jedis.close()
            }
        }

        adsClickCountDstream
    }
}
