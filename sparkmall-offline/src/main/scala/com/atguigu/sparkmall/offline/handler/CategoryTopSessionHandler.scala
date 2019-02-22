package com.atguigu.sparkmall.offline.handler

import com.atguigu.sparkmall.common.bean.UserVisitAction
import com.atguigu.sparkmall.common.util.JDBCUtil
import com.atguigu.sparkmall.offline.bean.CategoryCount
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object CategoryTopSessionHandler {

    def handle(sparkSession: SparkSession, userVisitActionRDD: RDD[UserVisitAction], taskId: String, top10CategoryList: List[CategoryCount])={
        val cidTop10: List[Long] = top10CategoryList.map(_.categoryId.toLong)
        val cidTop10BC: Broadcast[List[Long]] = sparkSession.sparkContext.broadcast(cidTop10)
        //根据需求一的结果保留点击top10的action（因为需求一的结果在Driver，而接下来的过滤操作在Executor，所有使用广播变量）
        val top10UserVisitActionRDD: RDD[UserVisitAction] = userVisitActionRDD.filter{userVisitAction =>
            cidTop10BC.value.contains(userVisitAction.click_category_id)
        }

        //统计每个session点击top10品类的次数
        val clickCountGroupByCidSessionRDD: RDD[(String, Long)] = top10UserVisitActionRDD.map(action =>
            (action.click_category_id+"_"+action.session_id, 1L)).reduceByKey(_+_)

        //按品类进行分组
        val sessionCountGroupByCidRDD: RDD[(String, Iterable[(String, Long)])] = clickCountGroupByCidSessionRDD.map { case (cidSession, count) =>
            val cidSessionArr: Array[String] = cidSession.split("_")
            val cid: String = cidSessionArr(0)
            val sessionId: String = cidSessionArr(1)
            (cid, (sessionId, count))
        }.groupByKey()
        sessionCountGroupByCidRDD

        //保留每组top10
        val sessionCountTop10RDD: RDD[Array[Any]] = sessionCountGroupByCidRDD.flatMap { case (cid, sessionCountIter) =>
            val sessionCountTop10List: List[(String, Long)] = sessionCountIter.toList.
              sortWith((sessionCount1, sessionCount2) => sessionCount1._2 > sessionCount2._2).take(10)
            //按照最终要保存的结构，调整结构
            val sessionCountTop10WithCidList: List[Array[Any]] = sessionCountTop10List.map { case (sessionId, clickcount) =>
                Array(taskId, cid, sessionId, clickcount)
            }
            sessionCountTop10WithCidList
        }

        //保存到mysql中
        val sessionCountTop10Arr: Array[Array[Any]] = sessionCountTop10RDD.collect()
        JDBCUtil.executeBatchUpdate("insert into category_top10_session_top10 values(?,?,?,?)", sessionCountTop10Arr)

    }
}
