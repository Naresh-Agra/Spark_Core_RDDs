package Spark_Core
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object Spark_Core {
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("SarkCore").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
    
    
    
    
  }
}