package com.atguigu.sparkmall.mock.util

import scala.collection.mutable.ListBuffer
import scala.util.Random

/**
  * 根据权重比例随机生成选项
  */
object RandomOptions {

    case class RanOpt[T](value: T, weight: Int)

    class RandomOptions[T](opts: RanOpt[T]*){
        var totalWeight = 0
        var optsBuffer = new ListBuffer[T]

        def getRandomOpt()={
            var randomNum = new Random().nextInt(totalWeight)
            optsBuffer(randomNum)
        }
    }

    def apply[T](opts: RanOpt[T]*): RandomOptions[T] ={
        val randomOptions: RandomOptions[T] = new RandomOptions[T]()
        for(opt <- opts){
            randomOptions.totalWeight += opt.weight
            for(i <- 1 to opt.weight){
                randomOptions.optsBuffer += opt.value
            }
        }
        randomOptions
    }

    def main(args: Array[String]): Unit = {
        val randomName: RandomOptions[String] = RandomOptions(RanOpt("a", 1), RanOpt("b", 300))
        for(i <- 1 to 40){
            println(i + ":" + randomName.getRandomOpt())
        }
    }
}
