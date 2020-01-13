package com.atguigu.sql.project

import org.apache.spark.sql.SparkSession

/**
  * Author atguigu
  * Date 2020/1/13 9:20
  */
object SqlApp {
    def main(args: Array[String]): Unit = {
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("SqlApp")
            .enableHiveSupport()
            .getOrCreate()
        import spark.implicits._
        spark.sql("use sql0830")
        spark.sql(
            """
              |select
              |    ci.*,
              |    pi.product_name,
              |    uv.click_product_id
              |from user_visit_action uv
              |join product_info pi on uv.click_product_id=pi.product_id
              |join city_info ci on uv.city_id=ci.city_id
            """.stripMargin).createOrReplaceTempView("t1")
        
        
        // 注册聚合函数
        spark.udf.register("remark", RemarkUDAF)
        spark.sql(
            """
              |select
              |    area,
              |    product_name,
              |    count(*) ct,
              |    remark(city_name) remark
              |from t1
              |group by area, product_name
            """.stripMargin).createOrReplaceTempView("t2")
        
        
        spark.sql(
            """
              |select
              |    *,
              |    rank() over(partition by area order by ct desc) rk
              |from t2
            """.stripMargin).createOrReplaceTempView("t3")
        
        
        spark.sql(
            """
              |select
              |    area,
              |    product_name,
              |    ct,
              |    remark
              |from t3
              |where rk<=3
            """.stripMargin).show(50, false)   // 只要碰到一个行动(show, save, insertInto, saveAsTable), 前面所有的sql都执行
        spark.close()
        
    }
}
/*
// 1. 先把需要的字段查出来  t1
select
    ci.*,
    pi.product_name,
    uv.click_product_id
from user_visit_action uv
join product_info pi on uv.click_product_id=pi.product_id
join city_info ci on uv.city_id=ci.city_id

// 2. 按照地区和商品进行分组聚合  t2
select
    area,
    product_name,
    count(*) ct // count(1) , sum(1)
from t1
group by area, product_name

// 3.每个地区取前3的商品  开窗 rank(row_number, dense_rank)函数   t3
select
    *,
    rank() over(partition by area order by ct desc) rk
from t2

// 4. 取前3
select
    area,
    product_name,
    ct
from t3
where rk<=3




地区	商品名称		点击次数	城市备注
华北	商品A		100000	北京21.2%，天津13.2%，其他65.6%
华北	商品P		80200	北京63.0%，太原10%，其他27.0%
华北	商品M		40000	北京63.0%，太原10%，其他27.0%
东北	商品J		92000	大连28%，辽宁17.0%，其他 55.0%

CREATE TABLE `user_visit_action`(
  `date` string,
  `user_id` bigint,
  `session_id` string,
  `page_id` bigint,
  `action_time` string,
  `search_keyword` string,
  `click_category_id` bigint,
  `click_product_id` bigint,
  `order_category_ids` string,
  `order_product_ids` string,
  `pay_category_ids` string,
  `pay_product_ids` string,
  `city_id` bigint)
row format delimited fields terminated by '\t';
load data local inpath '/opt/module/datas/user_visit_action.txt' into table user_visit_action;

CREATE TABLE `product_info`(
  `product_id` bigint,
  `product_name` string,
  `extend_info` string)
row format delimited fields terminated by '\t';
load data local inpath '/opt/module/datas/product_info.txt' into table product_info;

CREATE TABLE `city_info`(
  `city_id` bigint,
  `city_name` string,
  `area` string)
row format delimited fields terminated by '\t';
load data local inpath '/opt/module/datas/city_info.txt' into table city_info;
 */