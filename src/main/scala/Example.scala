package com.theotherandygrove

import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.{SparkConf, SparkContext}

object Example {

  def main(arg: Array[String]): Unit = {

    val conf = new SparkConf()
      .setAppName("Example")
      .setMaster("local[*]")

    val sc = new SparkContext(conf)

    val sqlContext = new SQLContext(sc)

    val schema = StructType(List(
      StructField("employee_id", IntegerType, true),
      StructField("salary", DecimalType(7,2), true))
    )

    // load initial RDD
    val rdd = sc.parallelize(List(
      Person(1, BigDecimal("50000.00")),
      Person(1, BigDecimal("123000.00")) //oops, this doesn't fit in a DecimalType(7,2)
    ))

    // convert to RDD[Row]
    val rowRdd = rdd.map(person => Row(person.id, person.salary))

    // convert to DataFrame (RDD + Schema)
    val dataFrame = sqlContext.createDataFrame(rowRdd, schema)

    // write to parquet
    val tableName: String = "emp_" + System.currentTimeMillis();
    dataFrame.write.parquet(tableName)

    // read from parquet
    sqlContext.read.parquet(tableName).show(10)

  }

}

case class Person(id: Int, salary: BigDecimal)







