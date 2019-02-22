package com.atguigu.sparkmall.offline.accumulator

import org.apache.spark.util.AccumulatorV2

import scala.collection.mutable

//定义累加器，输出类型Map[cid_actionType, count]，第一个参数为id加上某品类的动作（如id_click，id_order，id_pay），第二个参数为对应的次数
class CategoryAccumulator extends AccumulatorV2[String, mutable.HashMap[String, Long]]{
    var categoryCountMap = new mutable.HashMap[String, Long]()

    //是否为空
    override def isZero: Boolean = categoryCountMap.isEmpty

    //复制
    override def copy(): AccumulatorV2[String, mutable.HashMap[String, Long]] = {
        new CategoryAccumulator
    }

    //重置
    override def reset(): Unit = {
        categoryCountMap.clear()
    }

    //累加
    override def add(key: String): Unit = {
        categoryCountMap(key) = categoryCountMap.getOrElse(key, 0L) + 1L
    }

    //合并
    override def merge(other: AccumulatorV2[String, mutable.HashMap[String, Long]]): Unit = {
        val otherMap: mutable.HashMap[String, Long] = other.value
        categoryCountMap = categoryCountMap.foldLeft(otherMap) { case (otherMap, (key, count)) =>
            otherMap(key) = otherMap.getOrElse(key, 0L) + count
            otherMap
        }
    }

    override def value: mutable.HashMap[String, Long] = {
        categoryCountMap
    }
}
