package com.atguigu.sparkmall.realtime.app

import com.atguigu.sparkmall.common.util.MyKafkaUtil
import com.atguigu.sparkmall.realtime.bean.AdsLog
import com.atguigu.sparkmall.realtime.handler.{AreaCityAdsCountHandler, AreaTop3AdsHandler, BlackListHandler}
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.spark.streaming.dstream.{DStream, InputDStream}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

object RealtimeApp {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("realtime").setMaster("local[*]")
        val sc = new SparkContext(sparkConf)
        val ssc = new StreamingContext(sc, Seconds(5))

        sc.setCheckpointDir("./checkpoint")

        val recordDstream: InputDStream[ConsumerRecord[String, String]] = MyKafkaUtil.getKafkaStream("ads_log", ssc)
        val adsLogDstream: DStream[AdsLog] = recordDstream.map(_.value()).map { log =>
            val logArr: Array[String] = log.split(" ")
            AdsLog(logArr(0).toLong, logArr(1), logArr(2), logArr(3), logArr(4))
        }

        //需求四
        val filterdAdsLogDstream: DStream[AdsLog] = BlackListHandler.check(sc, adsLogDstream)
        BlackListHandler.handle(filterdAdsLogDstream)

        //需求五
        val areaCityAdsCountDstream: DStream[(String, Long)] = AreaCityAdsCountHandler.handle(filterdAdsLogDstream)

        //需求六
        AreaTop3AdsHandler.handle(areaCityAdsCountDstream)


        ssc.start()
        ssc.awaitTermination()
    }
}
