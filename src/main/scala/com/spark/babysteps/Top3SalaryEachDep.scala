package com.spark.babysteps

import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level

object Top3SalaryEachDep extends App {
  case class Employee(id: Int, name: String, depId: Int, salary: Double)
  case class Department(id: Int, name: String)
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
  org.apache.log4j.Logger.getLogger("org").setLevel(Level.ERROR)
  val spark = SparkSession.builder().master("local[2]").appName("TopEmployeeByDepartment").getOrCreate()
  import spark.implicits._

  val employeeDetails = spark.sparkContext.parallelize(employee)
  val departmentDetails = spark.sparkContext.parallelize(department)

  val employeeTop3 = employeeDetails.map(x => (x.depId, x)).groupByKey().flatMap(x => x._2.toList.sortBy(_.salary)(Ordering.Double.reverse).take(3))
  val employeeWithDep = employeeTop3.map(x => (x.depId, x)).join(departmentDetails.map(x => (x.id, x.name))).map(x => s"${x._2._1.name} - ${x._2._1.salary} - ${x._2._2}").collect

  employeeWithDep.foreach(println)
  spark.stop()

}