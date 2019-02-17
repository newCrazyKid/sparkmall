package com.atguigu.sparkmall.common.bean

/**
  * 用户访问动作表
  * @param date
  * @param user_id
  * @param session_id
  * @param page_id
  * @param action_time
  * @param search_keyword
  * @param click_category_id
  * @param click_product_id
  * @param order_category_ids
  * @param order_product_ids
  * @param pay_category_ids
  * @param pay_product_ids
  * @param city_id
  */
case class UserVisitAction(date: String,
                           user_id: Long,
                           session_id: String,
                           page_id: Long,
                           action_time: String,
                           search_keyword: String,
                           click_category_id: Long,
                           click_product_id: Long,
                           order_category_ids: String,
                           order_product_ids: String,
                           pay_category_ids: String,
                           pay_product_ids: String,
                           city_id: Long
                           )
