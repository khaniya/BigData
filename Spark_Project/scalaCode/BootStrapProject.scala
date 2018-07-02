import java.io._
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.{ DataFrame, SQLContext }
import scala.collection.mutable.HashMap
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.catalyst.expressions.aggregate.Average
import org.apache.spark.sql.SaveMode
import scala.math.BigDecimal
import scala.math.random
import au.com.bytecode.opencsv.CSVParser
import org.apache.spark._
import org.apache.spark.sql.functions._

case class Wages(experience: String, wages: Double)

object BootStrapProject extends App {
  override def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark and SparkSql").setMaster("local[*]")
    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")
    
    val csv = sc.textFile("/home/cloudera/Desktop/bootstrap/Wages1.csv")
    //csv.foreach(println)
    val headerAndRows = csv.map(line => line.split(",").map(_.trim))
    val header = headerAndRows.first
    val dataRDD = headerAndRows.filter(_(0) != header(0))
    
    val wages1 = dataRDD.map(p => Wages(p(1).toString(), p(4).toDouble))
    // Initialize an SQLContext
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._
    import sqlContext.implicits._
    
    val dataDF = wages1.toDF();
    val originalRDD1 = dataDF.groupBy("experience").agg(avg("wages"), variance("wages")).toDF
    val totalData = originalRDD1.collect()
    println("\n Mean and Variance from all data \n")
    println("Experience\t Mean\t Variance")
    totalData.foreach(x =>printf("%s\t%f\t%f \n", x(0), x(1).toString().toDouble, x(2).toString().toDouble))
    //writing in file
    originalRDD1.repartition(1).write.mode(SaveMode.Overwrite).format("csv").save("/home/cloudera/Desktop/bootstrap/experience_wages_original_output")
    val sampleRDD = dataDF.sample(false, 0.25);
    
    println("final result after resampling")
    
    val sum = (1 to 1000)
      .map(x => sampleRDD.sample(true, 1).groupBy("experience").agg(avg("wages"), variance("wages")).rdd)
      //.reduce(_ union _).filter(row => !row(2).toString().equals("NaN"))
      .flatMap(t => t.collect())
      .map(row => {
        if(row(2).toString.equals("NaN")){
          (row(0).toString, row(1).toString.toDouble, 0d)
        }else{
          (row(0).toString(), row(1).toString.toDouble, row(2).toString.toDouble)
        }
      })
      .toDF()
     //.map(row => (row(0).toString(), row(1).toString().toDouble, row(2).toString().toDouble)).toDF()
      
    sum.groupBy("_1").agg(mean("_2").alias("mean"), mean("_3").alias("variance")).withColumnRenamed("_1", "experience_year").show()
   // sum.repartition(1).write.mode(SaveMode.Overwrite).format("csv").save("/home/cloudera/Desktop/bootstrap/wages_final")
    println("Task Completed!")
  }
  
}
