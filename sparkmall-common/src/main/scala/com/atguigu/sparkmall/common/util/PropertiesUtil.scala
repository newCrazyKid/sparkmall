package com.atguigu.sparkmall.common.util

import java.io.InputStreamReader
import java.util.Properties

/**
  * 用来读取配置文件的工具
  */
object PropertiesUtil {

    def load(protertyName: String)={
        val props: Properties = new Properties()
        props.load(new InputStreamReader(Thread.currentThread().getContextClassLoader.getResourceAsStream(protertyName), "UTF-8"))
        props
    }

    def main(args: Array[String])={
        val properties: Properties = PropertiesUtil.load("config.properties")

        println(properties.getProperty("kafka.broker.list"))
    }
}
