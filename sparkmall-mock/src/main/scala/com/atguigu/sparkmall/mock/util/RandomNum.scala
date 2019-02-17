package com.atguigu.sparkmall.mock.util

import scala.util.Random

/**
  * 随机生成区间范围内的数字（左右均包含）
  */
object RandomNum {

    def apply(fromNum: Int, toNum: Int)={
        fromNum + new Random().nextInt(toNum - fromNum + 1)
    }

    def multi(fromNum: Int, toNum: Int, amount: Int, delimiter: String, canRepeat: Boolean)={
        //在fromNum和toNum之间的多个数组拼接的字符串 共amount个
        //用delimiter分割  canRepeat为false则不允许重复
        ""
    }
}
