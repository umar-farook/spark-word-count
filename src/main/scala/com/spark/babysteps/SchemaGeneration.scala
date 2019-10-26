package com.spark.babysteps

import org.apache.spark.sql.Encoders

object SchemaGeneration extends App {
  
  case class Employee(id:Int,name:String,salary:Long)
  val employeeSchema= Encoders.product[Employee].schema
  println(employeeSchema)
}