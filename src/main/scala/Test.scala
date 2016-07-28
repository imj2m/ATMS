import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD

/**
  * Created by MJ on 16. 5. 24..
  */
object Test extends App{

  val sc = new SparkContext("local[*]", "WordCountExample")

//  (A,“<Tom> <hasMother> <Jane>”,E1)
  val nodeRDD = sc.textFile("./source/sample_triples.txt").map(x => {val s = x.split('\t'); (s(0), s(1), s(2))})

//  (L2,[[A,B,R1,D,R2],[F]])
  val envRDD = sc.textFile("./source/sample_Env.txt").map(x => {val s = x.split('\t'); (s(0), s(1))})

//  (R1,“hasMother(?x,?y) ^ hasFather(?x,?z) :- hasSpouse(?y, ?z)”)
  val ruleRDD = sc.textFile("./source/sample_Rule.txt").map(x => {val s = x.split('\t'); (s(0), s(1))})

  show_all_belief(nodeRDD, envRDD).collect.foreach(println)

  def show_all_belief(nodeRDD: RDD[(String, String, String)], envRDD: RDD[(String, String)]) = {
    val joined_RDD = nodeRDD.map(t => (t._3, t)).join(envRDD).map(t => (t._2._1, t._2._2))
//    joined_RDD.foreach(println)

    joined_RDD
  }
}
