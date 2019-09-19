package Spark_Core
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession



object Spark_Core {
  def main(args:Array[String]):Unit={
    
    val conf = new SparkConf().setAppName("SarkCore").setMaster("local[*]")
    val sc=new SparkContext(conf)
    sc.setLogLevel("ERROR")
   val spark = SparkSession  // For importing spark implicts
				.builder()			
				.getOrCreate()
//    LOADING THE DATA
    
    println
    println("Loading the actual data to RDD")
    val data = sc.textFile("file:///E://Workouts//Data//sample_txns.txt")
    data.foreach(println)
    
    
//    println("Read Multiple text files to RDD")
//    val multidata=sc.textFile("file:///E://Workouts//Data//multidata1.txt,file:///E://Workouts//Data//multidata2.txt")
//    multidata.foreach(println)
//    
//    println("Read Json File to RDD")
//    println("Spark Core has not have an option to read the json file.")
//    println("It import the json file like as in the code and it will not import datawise.")
//    val jsondata=sc.textFile("file:///E://Workouts//Data//Multiarray.json")
//    jsondata.foreach(println)
    
    
// FILTER
    println
    println("Filtered Data")
    val fil_data =data.filter(x=>x.contains("Gymnastics"))
    fil_data.foreach(println)
    
// MAP
//    println
//    println("Mapped Data")
//    println("Map – convert data in the file into DataFrames")
//    import spark.implicits._
//    val header=fil_data.first
//    val datawh=fil_data.filter(x=>x!=header)
//    val Map_data =datawh.map(x=>x.split(",")).map(x=>schema(x(0),x(1),x(2),x(3),x(4),x(5),x(6),x(7),x(8),x(9))).toDF()
//    Map_data.foreach(println)
    
//    FlatMap
    println
    println("Flat_Mapped Data")
    val Map_data =fil_data.flatMap(x=>x.split(","))
    Map_data.foreach(println)
    
//    Union
    println
    println("Union Data")
    val union_data=data.union(fil_data)
    union_data.foreach(println)
    
//  Distint --Distinct function returns the distinct elements from the provided dataset.
    println
    val data_array = sc.parallelize(List(1,1,2,2,2,2,3,2,3,5,6,4,2,10,20,20,40))  
    val dist_data=data_array.distinct()
    dist_data.foreach(println)
    
    
//    PARTITIONS
    println
    println("Actual Data with partition size is not given")
    val data1 = sc.textFile("file:///E://Workouts//Data//sample_txns.txt")
    println(data1.partitions.size) 
    
    println("Partition size after the filter operation takes place")
    println(fil_data.partitions.size) 
    
    println("Actual Data with partition size given")
    val data2 = sc.textFile("file:///E://Workouts//Data//sample_txns.txt",10)
    println(data2.partitions.size)
    
    
// COALESCE---Coalesce is used to Reduce the size of partitions in the RDD
    
    println
    println("After Caolesce")
    val coalesce_data=data2.coalesce(5)
    println(coalesce_data.partitions.size)
    
    
// Repartition---Repartition is used to Increase/Decrease the No.Of Partitions in the RDD.
    println
    println("after Repartition applied")
    val repartition1=data2.repartition(3)        //Repartition
    println(repartition1.partitions.size)

// Few Actions   
    println
    println("Collect")
    val collect=fil_data.collect
    collect.foreach(println)
    println
    println("Count")
    println(data1.count())   //shows only the no.of occurence
    println
    println("Countbyvalue")
    val countbyvalue=data1.countByValue   //shows the no.of occurrence with values
    countbyvalue.foreach(println)
    println
    println("Take")
    val take=data1.take(5)  //shows top 5 rows in the table
    take.foreach(println)

    
  
  }
}