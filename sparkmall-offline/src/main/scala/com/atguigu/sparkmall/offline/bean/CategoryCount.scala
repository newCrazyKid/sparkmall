package com.atguigu.sparkmall.offline.bean

case class CategoryCount(taskId: String, categoryId: String, clickCount: Long, orderCount: Long, payCount: Long) {
//taskId:某个需求产生的数据，其他需求（的结果）也可能会用到，他们都源于相同的数据，使用这个属性，可以让其他需求直接用到之前需求的数据
}
