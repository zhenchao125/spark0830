package com.atguigu.sql.day02.hive

import org.apache.spark.sql.SparkSession

/**
  * Author atguigu
  * Date 2020/1/12 14:58
  */
object Hive {
    def main(args: Array[String]): Unit = {
        System.setProperty("HADOOP_USER_NAME", "atguigu")
        val spark: SparkSession = SparkSession
            .builder()
            .master("local[*]")
            .appName("Hive")
            .enableHiveSupport()  // 支持外置hive
            .config("spark.sql.warehouse.dir", "hdfs://hadoop102:9000/user/hive/warehouse/")
            // 如果想让spark写入的parquet的文件, 在hive中也可以查看到, 则关闭这个配置
            .config("spark.sql.hive.convertMetastoreParquet", false)
            .getOrCreate()
        import spark.implicits._
        
        //从外置hive读数据
//        spark.sql("use gmall")
//        spark.sql("select * from ads_uv_count").show
        
        //向hive写数据
//        val df = Seq(("lisi", 201), ("zs", 161)).toDF("name", "age")
        val df = Seq(( 202, "lisi"), (162, "zs")).toDF("age", "name")
        // 可以自动给我们创建表  如果是追加模式列名要保持一致, 顺序无所谓
        df.write.mode("append").saveAsTable("user0832")
        // 插入的时候表必须存在  不看名, 只用位置
//        df.write.insertInto("user0830")
        
        // 写sql方式进行写入数据
//        spark.sql("create table p2(name string, age int)")
//        spark.sql("insert into table p1 values('abc', 10)")
//        spark.sql("select * from p1").show
//        df.createOrReplaceTempView("pp")
//        spark.sql(
//            """
//              |insert overwrite table p2
//              |select name, age from pp
//            """.stripMargin).show
        
//        spark.sql("create database test88").show
        
        spark.close()
    }
}
/*
1. hive-site.xml文件copy
2. mysql 驱动
3. 依赖:spark-hive依赖
4. .enableHiveSupport()
 */