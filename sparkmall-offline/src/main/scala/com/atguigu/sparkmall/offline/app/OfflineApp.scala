package com.atguigu.sparkmall.offline.app

import java.util.{Properties, UUID}

import com.alibaba.fastjson.{JSON, JSONObject}
import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.{JDBCUtil, PropertiesUtil}
import com.atguigu.sparkmall.offline.accumulator.CategoryAccumulator
import com.atguigu.sparkmall.offline.bean.CategoryCount
import com.atguigu.sparkmall.offline.handler.{CategoryCountHandler, CategoryTopSessionHandler}
import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

import scala.collection.mutable

object OfflineApp {

    def main(args: Array[String]): Unit = {
        val taskId: String = UUID.randomUUID().toString
        
        val sparkConf: SparkConf = new SparkConf().setAppName("sparkmall-offline").setMaster("local[*]")
        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()

        val userVisitActionRDD: RDD[UserVisitAction] = readUserVisitActionToRDD(sparkSession)

        //需求一
        val categoryCountList: List[CategoryCount] = CategoryCountHandler.handler(sparkSession, userVisitActionRDD, taskId)
        println("需求一完成！")

        //需求二
        CategoryTopSessionHandler.handle(sparkSession, userVisitActionRDD, taskId, categoryCountList)
        println("需求二完成！")

    }

    /**
      * 根据配置文件中的条件，读取hive中符合条件的访问日志，并生成RDD
      */
    def readUserVisitActionToRDD(sparkSession: SparkSession)={
        val properties: Properties = PropertiesUtil.load("conditions.properties")
        val conditionJson: String = properties.getProperty("condition.params.json")
        val conditionJsonObj: JSONObject = JSON.parseObject(conditionJson)
        val startDate: String = conditionJsonObj.getString("startDate")
        val endDate: String = conditionJsonObj.getString("endDate")
        val startAge: String = conditionJsonObj.getString("startAge")
        val endAge: String = conditionJsonObj.getString("endAge")

        var sql = new StringBuilder("select v.*  from user_visit_action v,user_info u where v.user_id=u.user_id")
        if (startDate.nonEmpty) {
            sql.append(" and date>='" + startDate + "'")
        }
        if (endDate.nonEmpty) {
            sql.append(" and date<='" + endDate  + "'")
        }
        if (startAge.nonEmpty) {
            sql.append(" and age>=" + startAge  )
        }
        if (endAge.nonEmpty) {
            sql.append(" and age<=" + endAge )
        }

        println(sql)

        sparkSession.sql("use sparkmall")

        import sparkSession.implicits._
        //在转换成RDD之前先转换成DataSet，使得有了UserVisitAction类型
        val rdd: RDD[UserVisitAction] = sparkSession.sql(sql.toString()).as[UserVisitAction].rdd

        rdd
    }
}
