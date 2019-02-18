package com.atguigu.sparkmall.mock.util

import scala.collection.mutable
import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * 随机生成区间范围内的数字（左右均包含）
  */
object RandomNum {

    def apply(fromNum: Int, toNum: Int)={
        fromNum + new Random().nextInt(toNum - fromNum + 1)
    }

    def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean)={
        //在fromNum到toNum之间的多个数字拼接的字符串 共amount个
        //用delimiter分隔符拼接  canRepeat为false则不允许重复
        if(canRepeat) {
            val numList = new ListBuffer[Int]()
            while (numList.size < amount) {
                numList += apply(fromNum, toNum)
            }
            numList.mkString(delimiter)
        }else{
            val numSet = new mutable.HashSet[Int]()
            while(numSet.size < amount){
                numSet += apply(fromNum, toNum)
            }
            numSet.mkString(delimiter)
        }

    }

    def main(args: Array[String]): Unit = {
        val str: String = multi(2, 10, 5,",", false)
        println(str)
    }
}
