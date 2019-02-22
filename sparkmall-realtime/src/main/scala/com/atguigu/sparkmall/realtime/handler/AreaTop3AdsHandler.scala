package com.atguigu.sparkmall.realtime.handler

import com.atguigu.sparkmall.common.util.RedisUtil
import org.apache.spark.streaming.dstream.DStream
import org.json4s.native.JsonMethods
import org.json4s.JsonDSL._
import redis.clients.jedis.Jedis

object AreaTop3AdsHandler {

    def handle(areaCityAdsCountDstream: DStream[(String, Long)])={
        //将需求五的结果去掉城市，再聚合得到，每天每地区每广告的点击数：day_area_city_adskey:count => day_area_adskey:count
        val areaAdsCountDstream: DStream[(String, Long)] = areaCityAdsCountDstream.map { case (day_area_city_adskey, count) =>
            val keyArr: Array[String] = day_area_city_adskey.split(":")
            val daykey: String = keyArr(0)
            val area: String = keyArr(1)
            val city: String = keyArr(2)
            val adsId: String = keyArr(3)
            (daykey + ":" + area + ":" + adsId, count)
        }.reduceByKey(_ + _)

        //将kv结构变成层级结构：RDD[day_area_adskey:count] => RDD[(day,(area, (adskey, count)))] => RDD[(day, Iterable(area, (adskey, count)))]
        //=> map[area, Iterable(area, (adskey, count))] => map[area, map[adskey, count]]
        //=>目标：daykey : [map[area: adsTop3map[adsId : count]]] => daykey : json数据
        val areaAdsCountGroupByDayDStream: DStream[(String, Iterable[(String, (String, Long))])] = areaAdsCountDstream.map { case (day_area_adskey, count) =>
            val keyArr: Array[String] = day_area_adskey.split(":")
            val daykey: String = keyArr(0)
            val area: String = keyArr(1)
            val adsId: String = keyArr(2)
            (daykey, (area, (adsId, count)))
        }.groupByKey()

        //按地区分组
        val areaAdsTop3JsonGroupByDayDSream: DStream[(String, Map[String, String])] = areaAdsCountGroupByDayDStream.map { case (daykey, areaIter) =>
            val adsCountGroupByAreaMap: Map[String, Iterable[(String, (String, Long))]] = areaIter.groupBy { case (area, (adsId, count)) => area }
            val adsTop3CountGroupByAreaMap: Map[String, String] = adsCountGroupByAreaMap.map { case (area, adsIter) =>
                //去掉iterable中的area冗余，降序获取前三
                val top3AdsList: List[(String, Long)] = adsIter.map { case (area, (adsId, count)) => (adsId, count) }.toList.sortWith(_._2 > _._2).take(3)
                //变成json
                val top3AdsJsonString: String = JsonMethods.compact(JsonMethods.render(top3AdsList))
                (area, top3AdsJsonString)
            }
            (daykey, adsTop3CountGroupByAreaMap)
        }

        //保存到redis中
        areaAdsTop3JsonGroupByDayDSream.foreachRDD{rdd =>
            rdd.foreachPartition{dayIter =>
                val jedisClient: Jedis = RedisUtil.getJedisClient()
                dayIter.foreach{case (daykey, areaMap) =>
                    import collection.JavaConversions._
                    jedisClient.hmset("area_top3_ads:" + daykey, areaMap)
                }
                jedisClient.close()
            }
        }

    }
}
