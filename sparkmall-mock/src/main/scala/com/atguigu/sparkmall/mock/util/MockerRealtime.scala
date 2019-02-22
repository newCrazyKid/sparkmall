package com.atguigu.sparkmall.mock.util

import java.util.Properties

import com.atguigu.sparkmall.common.bean.CityInfo
import com.atguigu.sparkmall.common.util.PropertiesUtil
import com.atguigu.sparkmall.mock.util.RandomOptions.RanOpt
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.collection.mutable.ArrayBuffer
import scala.util.Random

object MockerRealtime {

    /**
      * 模拟数据
      * 格式：timestamp area city userid adid
      *         时间点  地区 城市 用户   广告
      */
    def generateMockData()={
        val array: ArrayBuffer[String] = ArrayBuffer[String]()
        val cityRandomOpt = RandomOptions(RanOpt(CityInfo(1, "北京", "华北"), 30),
            RanOpt(CityInfo(1, "上海", "华东"), 30),
            RanOpt(CityInfo(1, "广州", "华南"), 10),
            RanOpt(CityInfo(1, "深圳", "华南"), 20),
            RanOpt(CityInfo(1, "天津", "华北"), 10))

        val random = new Random()

        for(i <- 0 to 50){
            val timestamp: Long = System.currentTimeMillis()
            val cityInfo: CityInfo = cityRandomOpt.getRandomOpt()
            val area: String = cityInfo.area
            val city: String = cityInfo.city_name
            val userid: Int = random.nextInt(6) + 1
            val adid: Int = random.nextInt(6) + 1

            array += timestamp + " " + area + " " + city + " " + userid + " " + adid
        }
        array.toArray
    }

    def createKafkaProducer(broker: String)={
        val properties = new Properties()
        //添加配置
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, broker)
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
        //根据配置创建Kafka生产者
        new KafkaProducer[String, String](properties)
    }

    def main(args: Array[String]): Unit = {
        val properties: Properties = PropertiesUtil.load("config.properties")
        val broker: String = properties.getProperty("kafka.broker.list")
        val topic = "ads_log"

        //创建Kafka生产者
        val kafkaProducer: KafkaProducer[String, String] = createKafkaProducer(broker)
        while(true){
            //随机生成实时数据，并通过Kafka生产者发送到Kafka集群
            for(line <- generateMockData()){
                kafkaProducer.send(new ProducerRecord[String, String](topic, line))
                println(line)
            }
            Thread.sleep(2000)
        }
    }
}
