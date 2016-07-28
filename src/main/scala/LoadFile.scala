/**
  * Created by MJ on 16. 5. 29..
  */


import com.typesafe.config.{Config, ConfigFactory}
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import spark.jobserver.{SparkJob, SparkJobInvalid, SparkJobValid, SparkJobValidation}
import scala.util.Try

object LoadFile extends SparkJob{
  def main(args: Array[String]) {
    val sc = new SparkContext("local[*]", "ATMS")
    val config = ConfigFactory.parseString("")
    val results = runJob(sc, config)
    println("Result is " + results)
  }

  def validate(sc: SparkContext, config: Config): SparkJobValidation = {
    Try(config.getString("path"))
      .map(x => SparkJobValid)
      .getOrElse(SparkJobInvalid("No path config param"))

  }

  def runJob(sc: SparkContext, config: Config): Any = {
    val filePath = config.getString("path")

    val nodeRDD = sc.textFile(filePath).map(x => {val s = x.split('\t'); (s(0), s(1), s(2))})

    val result = nodeRDD.take(1)
    result
  }
}
