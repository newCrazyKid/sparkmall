package com.atguigu.sparkmall.offline.handler

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import com.atguigu.sparkmall.offline.accumulator.CategoryAccumulator
import com.atguigu.sparkmall.offline.bean.CategoryCount
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object CategoryCountHandler {

    def handler(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String)={
        val accumulator = new CategoryAccumulator
        sparkSession.sparkContext.register(accumulator)

        //遍历RDD，注意订单、支付可能涉及多个品类，需要根据逗号拆分，再分别累加
        userVisitActionRDD.foreach { userVisitAction =>
            if (userVisitAction.click_category_id != -1) {
                val key: String = userVisitAction.click_category_id + "_click"
                accumulator.add(key)
            } else if (userVisitAction.order_category_ids != null && userVisitAction.order_category_ids.length > 0) {
                val orderIds: Array[String] = userVisitAction.order_category_ids.split(",")
                for (id <- orderIds) {
                    val key: String = id + "_order"
                    accumulator.add(key)
                }
            } else if (userVisitAction.pay_category_ids != null && userVisitAction.pay_category_ids.length > 0) {
                val payIds: Array[String] = userVisitAction.pay_category_ids.split(",")
                for (id <- payIds) {
                    val key: String = id + "_pay"
                    accumulator.add(key)
                }
            }
        }

        //得到累加器结果
        val categoryCountMap: mutable.HashMap[String, Long] = accumulator.value
//        println(s"categoryMap = ${categoryCountMap.mkString("\n")}")

        //把map中的key中的相同id进行聚合
        val categoryGroupByIdMap: Map[String, mutable.HashMap[String, Long]] = categoryCountMap.groupBy{case (key, count) => key.split("_")(0)}
//        println("categoryGroupByIdMap: " + categoryGroupByIdMap.mkString("\n"))

        //聚合成List[CategoryCount]
        val categoryCountList: List[CategoryCount] = categoryGroupByIdMap.map { case (id, actionMap) =>
            CategoryCount("", id, actionMap.getOrElse(id + "_click", 0L), actionMap.getOrElse(id + "_order", 0L), actionMap.getOrElse(id + "_pay", 0L))
        }.toList

        //根据结果排序（此处先按照下单数降序，再按照点击数降序），并截取前十
        val top10CategoryCountList: List[CategoryCount] = categoryCountList.sortWith { (categoryCount1, categoryCount2) =>
            if (categoryCount1.orderCount > categoryCount2.orderCount) {
                true
            } else if (categoryCount1.orderCount == categoryCount2.orderCount) {
                if (categoryCount1.clickCount > categoryCount2.clickCount) {
                    true
                } else {
                    false
                }
            } else {
                false
            }
        }.take(10)

//        println("top10CategoryCountList: " + top10CategoryCountList.mkString("\n"))

        //保存到mysql
        val resultList: List[Array[Any]] = top10CategoryCountList.map{categoryCount => Array(taskId, categoryCount.categoryId, categoryCount.clickCount, categoryCount.orderCount, categoryCount.payCount)}
        JDBCUtil.executeBatchUpdate("insert into category_top10 values(?,?,?,?,?)", resultList)

        top10CategoryCountList
    }
}
