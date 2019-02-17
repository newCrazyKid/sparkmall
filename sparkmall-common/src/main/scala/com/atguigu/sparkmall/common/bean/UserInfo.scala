package com.atguigu.sparkmall.common.bean

/**
  * 用户信息表
  * @param user_id
  * @param username
  * @param name
  * @param age
  * @param professional
  * @param gender
  */
case class UserInfo(user_id: Long,
                    username: String,
                    name: String,
                    age: Int,
                    professional: String,
                    gender: String
                   )
