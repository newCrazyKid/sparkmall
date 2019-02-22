package com.atguigu.sparkmall.offline.app

import java.util.Properties

import com.atguigu.sparkmall.common.util.PropertiesUtil
import com.atguigu.sparkmall.offline.udf.CityRatioUDAF
import org.apache.spark.SparkConf
import org.apache.spark.sql.{SaveMode, SparkSession}

object AreaCountApp {

    def main(args: Array[String]): Unit = {
        val sparkConf: SparkConf = new SparkConf().setAppName("areaCount").setMaster("local[*]")
        val sparkSession: SparkSession = SparkSession.builder().config(sparkConf).enableHiveSupport().getOrCreate()
        val properties: Properties = PropertiesUtil.load("config.properties")

        sparkSession.udf.register("city_ratio", new CityRatioUDAF)

        sparkSession.sql("use sparkmall")
        //关联用户动作表和城市表
        sparkSession.sql("select c.area,c.city_name,click_product_id from user_visit_action u inner join city_info c where u.city_id=c.city_id and click_product_id > 0").
          createOrReplaceTempView("action_city_view")
        //按照地区+商品进行分组
        sparkSession.sql("select area,click_product_id,count(*) clickcount,city_ratio(city_name) city_remark from action_city_view group by area,click_product_id").
          createOrReplaceTempView("area_product_clickcount_view")
        //取出商品在地区的排名
        sparkSession.sql("select area,click_product_id,clickcount,city_remark from (select v.*,rank() over(partition by area order by clickcount desc) rk from area_product_clickcount_view v) clickrk where rk <= 3").
          createOrReplaceTempView("area_product_clickcount_top3_view")
        //获取前三的商品，取到商品名
        sparkSession.sql("select area,p.product_name,clickcount,city_remark from area_product_clickcount_top3_view t3,product_info p where t3.click_product_id=p.product_id").
          write.format("jdbc").option("url", properties.getProperty("jdbc.url")).option("user", properties.getProperty("jdbc.user")).
          option("password", properties.getProperty("jdbc.password")).option("dbtable", "area_count_info").mode(SaveMode.Append).save()

    }
}
