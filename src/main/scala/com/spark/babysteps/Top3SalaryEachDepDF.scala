package com.spark.babysteps

import org.apache.log4j.Level
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.expressions.WindowSpec
import org.apache.spark.sql.functions._

object Top3SalaryEachDepDF extends App {

  org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[2]").appName("WordCount").getOrCreate();
  import spark.implicits._
  case class Employee(id: Int, name: String, depId: Int, salary: Double)
  case class Department(depId: Int, depName: String)
  def initializeData(): (List[Employee], List[Department]) = {
    (List(
      Employee(1, "Employee 1", 1, 10000),
      Employee(2, "Employee 2", 1, 8000),
      Employee(3, "Employee 3", 1, 12000),
      Employee(4, "Employee 4", 1, 4000),
      Employee(5, "Employee 5", 1, 34000),
      Employee(6, "Employee 6", 2, 4000),
      Employee(7, "Employee 7", 2, 8000),
      Employee(8, "Employee 8", 2, 6000),
      Employee(9, "Employee 9", 2, 40000),
      Employee(10, "Employee 10", 2, 12000),
      Employee(11, "Employee 11", 3, 4000),
      Employee(12, "Employee 12", 3, 8000),
      Employee(13, "Employee 13", 3, 6000),
      Employee(14, "Employee 14", 3, 40000),
      Employee(15, "Employee 15", 3, 12000)), List(Department(1, "Department 1"), Department(2, "Department 2"), Department(3, "Department 3")))
  }

  val (employee: List[Employee], department: List[Department]) = initializeData()

  val employeeDf = spark.createDataFrame(employee)
  val departmentDf = spark.createDataFrame(department)

  employeeDf.printSchema()
  departmentDf.printSchema()

  val employeeJoined = employeeDf.join(departmentDf, Seq("depId"))
  val spec = Window.partitionBy("depId").orderBy(col("salary").desc)

  //With df
  val result = employeeJoined.select($"*", rank().over(spec).as("rank")).filter($"rank" <= 3)
  println("With DF")
  result.show
  //with sql
  employeeJoined.createOrReplaceTempView("employee")
  val resultSql = spark.sql("select *, (rank() over(partition by depId order by salary desc)) as rank from employee").filter($"rank" <= 3)
  println("With sql")
  resultSql.show
  spark.stop
}