import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.immutable.List.empty

/**
  * Created by batselemjagvaral1 on 2/13/16.
  */


object AtmsBuilder{

  var assumpCount = 0
  var justCount = 0

  var sc: SparkContext = null

  var indexedHolds: IndexedRDD[String, Set[Set[Int]]] = null
  var indexedAssumptions: IndexedRDD[Long, String] = null
  var indexedJustificands: IndexedRDD[Long, (String, List[String])] = null
  var indexedEnvs: IndexedRDD[String, Boolean] = null
  var indexedJustifiers: IndexedRDD[String, Iterable[Long]] = null

  var nogoodListInt: Iterable[List[Int]] = Iterable.empty
  var nogoodList: Set[Set[Int]] = Set.empty

  def crossUnion( label1: Set[Set[Int]], label2: Set[Set[Int]] ): Set[Set[Int]] = {
    var list: Set[Set[Int]] = Set.empty
    for (e1 <- label1; e2 <- label2) {
      val e = e1.union(e2)
      if ( !list.contains(e) ) list = list + e
    }
    list
  }

  def minimality( label: Set[Set[Int]] ) =
    if (label.size == 1) {
      label
    } else {
      var newset: Set[Set[Int]] = Set.empty

      for (e1 <- label) {
        var flag = true
        for (e2 <- label)
          if (e2.subsetOf(e1) && !e1.equals(e2)) {
            flag = false
          }
        if ( flag ) newset = newset + e1
      }
      newset
    }

  def parseJustification(line: String) = {
    var antecedents: List[String] = empty
    // Get subject
    var rest = line.substring(2)

    while (rest.indexOf(',') < rest.indexOf(")")) {

      val a = rest.substring(0, rest.indexOf(',') ).trim()
      antecedents = antecedents :+ a
      rest = rest.substring(rest.indexOf(',') + 1)
    }

    val a = rest.substring(0, rest.indexOf(')') ).trim()
    antecedents = antecedents :+ a
    rest = rest.substring(rest.indexOf(')') + 2 )

    val conclusion = rest.substring(0, rest.indexOf(')') ).trim()
    (antecedents, conclusion)
  }

  def parseTriple(triple: String) = {
    // Get subject
    val subj = if (triple.startsWith("<")) {
      triple.substring(0, triple.indexOf('>') + 1)
    } else {
      triple.substring(0, triple.indexOf(' '))
    }

    // Get predicate
    val triple0 = triple.substring(triple.indexOf(' ') + 1)
    val pred = triple0.substring(0, triple0.indexOf('>') + 1)

    val triple1 = triple0.substring(pred.length + 1)

    // Get object
    val obj = if (triple1.startsWith("<")) {
      triple1.substring(0, triple1.indexOf('>') + 1)
    }
    else if (triple1.startsWith("\"")) {
      triple1.substring(0, triple1.substring(1).indexOf('\"') + 2)
    }
    else {
      triple1.substring(0, triple1.indexOf(' '))
    }

    (subj, pred, obj)
  }

  def throwsException(msg: String) {
    throw new IllegalStateException(msg);
  }

  def joinAssumption(If: List[String]) = {
    var envList: Set[Set[Int]] = indexedHolds.get(If.head).get
    for (n <- If.tail) {
      val envs = indexedHolds.get(n)
      if (!envs.equals(None)) {
        envList = crossUnion(envList, envs.get)
      } else {
        throwsException("Create your assumption!")
      }
    }
    envList
  }

  def removeInconsistency(envs: Set[Set[Int]]) = {

    /*
    indexedEnvs = indexedEnvs.multiput(envs.map(e => (e.mkString(","), false)).toMap)
    */
    val bcNogoods: Broadcast[Set[Set[Int]]] = sc.broadcast(envs)

    val retractNodeNames: Array[String] = indexedHolds.mapPartitions(iter => {
      val nogoods: Set[Set[Int]] = bcNogoods.value
      for {
        (node, label) <- iter
        if (!label.forall(e => nogoods.forall(nogood => !nogood.subsetOf(e))) && !node.equals("false"))
      } yield node
    }, true).collect()

    val retractNodes = indexedHolds.multiget(retractNodeNames)

    val newLabels: Map[String, Set[Set[Int]]] = retractNodes.map { case (node, label) =>
      val nogoods: Set[Set[Int]] = bcNogoods.value
      (node, label.filter(e => nogoods.forall(nogood => !nogood.subsetOf(e))))
    }

    indexedHolds = indexedHolds.multiput(newLabels)
  }

  def subsetListInt(e1: Set[Int], e2: Set[Int]): Boolean = {
    e1.subsetOf(e2)
  }

  def condBottomUpdate(nogoodList: Set[Set[Int]]) = {
    /*
    val bcNogoodList = sc.broadcast(nogoodList)
    val envs = indexedEnvs.mapPartitions(iter => {
      val nogoods = bcNogoodList.value
      for {
        (e, flag) <- iter
        if (!nogoods.forall(nogood => !subsetListInt(nogood, e.split(",").map(s => s.toInt).toSet)) && flag)
      } yield e.split(",").map(s => s.toInt).toSet
    }, true).collect().toSet
    */
    removeInconsistency(nogoodList)
  }

  def condUpdatePropagate(node: String, envList: Set[Set[Int]]) = {
    indexedHolds = indexedHolds.put(node, envList)
    if (node.trim().equals("false")) {
      condBottomUpdate(envList)

    } else {
      val justs = indexedJustifiers.get(node)
      if (!justs.equals(None)) {
        for (j <- justs.get) {
          nodeUpdateEnv(j)
        }
      }
    }
  }

  def nodeUpdateEnv(Just: Long) {
    val (node, _if) = indexedJustificands.get(Just).get
    val oldEnvList = indexedHolds.get(node)
    if (!oldEnvList.equals(None)) {
      val envList = joinAssumption(_if)

      val newEnvList = minimality((oldEnvList.get ++ envList))

      if (!envList.equals(oldEnvList)) {
        condUpdatePropagate(node, newEnvList)
      }
    } else {
      val envList = joinAssumption(_if)
      condUpdatePropagate(node, envList)
    }
  }

  def declareAssumption(a: String): Unit = {
    val newLabel = Set(assumpCount)
    indexedAssumptions = indexedAssumptions.put(assumpCount, a)
    indexedHolds = indexedHolds.put(a, Set(newLabel))
    assumpCount += 1
  }

  def atmsAddJust(antecedents: Array[String], conclusion: String) {
    try {
      val newJust = justCount + 1L
      indexedJustificands = indexedJustificands.put(newJust, (conclusion, antecedents.toList))
      for (inNode <- antecedents) {
        val oldInNnode = indexedJustifiers.get(inNode)
        if (!oldInNnode.equals(None)) {
          indexedJustifiers = indexedJustifiers.put(inNode, (oldInNnode.get.toList :+ newJust).distinct)
        }
      }
      nodeUpdateEnv(newJust)
    } catch {
      case e: Exception => println("exception caught : " + e)
    }
  }

  def setLogLevel(level: Level): Unit = {
    Logger.getLogger("org").setLevel(level)
    Logger.getLogger("akka").setLevel(level)
  }

  def createAtms(fileName: String): Unit = {

    sc = new SparkContext({
      new SparkConf()
        .setAppName("ATMS")
        .setMaster("local[*]")
//        .set("spark.io.compression.codec", "org.apache.spark.io.LZ4CompressionCodec")
//        .set("spark.broadcast.compress", "true")
//        .set("spark.locality.wait", "10000")
//        .set("spark.shuffle.compress", "true")
//        .set("spark.rdd.compress", "true")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    })
    setLogLevel(Level.ERROR)

    val redudTriple = "<http://www.w3.org/2002/07/owl#Thing> type <http://www.w3.org/2000/01/rdf-schema#Class>"

    val justifications0: Array[(List[String], String)] = sc.objectFile(fileName).take(10000)//.collect()
    def cleanse(str: String) = str.substring(1, str.length - 1)
    val justifications = sc.parallelize(justifications0).map { case (ant, con) => (ant.map(a => cleanse(a)), cleanse(con))}

    justifications.take(20).foreach(println)
    //val justifications: RDD[(List[String], String)] = sc.parallelize(List(
    //  (List("a"), "e"),
     //   (List("e"), "e"))
    //)
//    justifications.filter { case (cons, con) => cons.contains(con) }.collect.foreach(println)

    println("input justifications : " + justifications.count())

    val inferences = justifications
         .filter{ case (_, cons) =>

              !cons.contains(redudTriple) &&
              !cons.trim().equals("") &&
              !cons.contains("type <http://www.w3.org/2000/01/rdf-schema#Class>") &&
              !cons.contains("type <http://www.w3.org/2000/01/rdf-schema#Literal>") &&
              !cons.contains("type <http://www.w3.org/2002/07/owl#ObjectProperty>") &&
              !cons.contains("type <http://www.w3.org/1999/02/22-rdf-syntax-ns#List>") &&
              !cons.contains("type <http://www.w3.org/2002/07/owl#Thing>") &&
              !cons.contains("type <http://www.w3.org/2001/XMLSchema#string>") &&
              !cons.startsWith("type") &&
              !cons.startsWith("\"")

          }.map { case (ants, cons) => (ants.map(a => a.trim), cons.trim)}.zipWithIndex()

    println("filtered justifications: " + inferences.count())
    justifications.unpersist()

    val partitioner = new HashPartitioner(300)

    val ants = inferences.flatMap { case ((antecedents, _), _) => antecedents.map(a => a.trim()) }
    val conc = inferences.map { case ((_, conclusion), _) => conclusion.trim() }
    val assumptions = ants.subtract(conc).distinct.zipWithUniqueId().partitionBy(partitioner)

    justCount = inferences.count().toInt

    val startTime = System.currentTimeMillis()

    val justifiers: RDD[(String, Iterable[Long])] = inferences
      .flatMap { case ((_if, _), i) => _if.map(a => (a, i)) }
      .groupByKey()
      .partitionBy(partitioner)
      //indexedJustifiers = IndexedRDD( justifiers )

    val justificands: RDD[(Long, (String, List[String]))] = inferences
      .map { case ((_if, _then), justId) => (justId, (_then, _if)) }
      .partitionBy(partitioner)

    var tempJustificands = justificands

    var holds: RDD[(String, Set[Set[Int]])] = assumptions
      .map { case (a, id) => (a, Set(id.toInt)) }
      .groupByKey()
      .map { case (a, label) => (a, label.toSet)}
      .partitionBy(partitioner)

    var iteration = 0
    var oldCount = -1L

    var newCount = 1L

    while( newCount > 0 ) {
      oldCount = tempJustificands.count()
      val labels: RDD[((Long, String), (Iterable[Set[Int]], String))] = tempJustificands
        .flatMap { case (just, (_then, _if)) => _if.map(n => (n, (just, _then))) }
        .leftOuterJoin(holds)
        .map { case (node, ((just, _then), label)) =>
          if (label.equals(None)) {
            ((just, _then), (Iterable.empty, node))
          } else {
            ((just, _then), (label.get, node))
          }
        }

      println("labels: " + labels.count())

      val restJust: RDD[(Long, Null)] = labels
        .filter { case ((just, _then), (label, node)) => label.isEmpty }
        .map { case ((just, _), _) => (just, null) }
        .reduceByKey((x, y) => x)
        .partitionBy(partitioner)

      println("restJust: " + restJust.count())

      tempJustificands = tempJustificands
        .leftOuterJoin(restJust)
        .filter { case (_, (_, option)) => !option.equals(None) }
        .map { case (just, (ifThen, _)) => (just, ifThen) }
        .partitionBy(partitioner)
      newCount = tempJustificands.count()

      println("tempJustificands: " + newCount)

      val newHolds: RDD[(String, Set[Set[Int]])] = labels
        .map { case  ((just, _then), (label, node)) => ((just, _then), label.toSet) }
        .reduceByKey(crossUnion).map { case ((_, node), label) => (node, label) }
        .partitionBy(partitioner)
        .filter{ case (n,l) => !l.isEmpty }
      newCount = newHolds.count()
      println("newHolds: " + newHolds.count())

      val holds1 = holds.union(newHolds)

      holds1.count()
      println("holds1 ...")

      holds = holds1.reduceByKey((label1, label2) => label1 ++ label2 ).partitionBy( partitioner )

      iteration += 1
      println("holds: " + holds.count())
      println("iteration : " + iteration)

      if (iteration > 10)  {
        newCount = 0
      }
      if ( iteration > 2) {
        newHolds.take(10).foreach(println)
      }
    }

    nogoodList = holds
      .filter { case (node, _) => node.equals("false") }
      .flatMap { case (_, label) => label }
      .collect.toSet

    def removeInconsistency(label: Set[Set[Int]]) = {
      label.filter( e => nogoodList.forall(nogood => nogood.subsetOf(e) ) )
    }

    val environments: RDD[(Set[Int], Long)] =
      holds.flatMap { case (_, label) => label }
        .distinct()
        .zipWithUniqueId()

    environments.count()


//    val nogoodEnvs = environments
//      .filter { case (_, flag) => !flag }
//      .map { case (e, _) => e }.collect()
//
//
//    val bcNogoodEnvs = sc.broadcast(nogoodEnvs)
//
//
//    holds = holds.map { case (node, label) =>
//      val newLabel = label.filter(e => bcNogoodEnvs.value.forall(nogood => !nogood.equals(nogood & e)))
//      (node, newLabel)
//    }



    val indexedNodes: RDD[(String, Long)] = holds.map{ case (nodename, _) => nodename }.distinct().zipWithUniqueId()
    indexedNodes.saveAsObjectFile("output/indexedNodes")
    holds.count()
    holds.saveAsObjectFile("output/holds")
    assumptions.map(x =>(x._2, x._1)).partitionBy(partitioner).saveAsObjectFile("output/assumptions")
    justificands.saveAsObjectFile("output/justs")
    environments.map { case (env, index) => (env.mkString(","), index) }.saveAsObjectFile("output/envs")
    justifiers.saveAsObjectFile("output/justifiers")

    val justifieds: RDD[(String, Iterable[Long])] = justificands.map{ case (id, (cons, ants)) => (cons, id)}.groupByKey()
    justifieds.saveAsObjectFile("output/justifieds")



//    indexedHolds = IndexedRDD( holds )
//    indexedHolds.count()
//    indexedAssumptions = IndexedRDD( assumptions.map(x =>(x._2, x._1)).partitionBy(partitioner) )
//    indexedAssumptions.count()
//    indexedJustificands = IndexedRDD( justificands )
//    indexedJustificands.count()
//    indexedEnvs = IndexedRDD( environments.map { case (e, flag) => (e.mkString(","), flag)} )
//    indexedEnvs.count()
//    val endTime = System.currentTimeMillis()
//    assumpCount = assumptions.count().toInt



  }

  def main(args: Array[String]): Unit = {


      val filename = "/Users/MJ/Desktop/bsh_output4"
      createAtms(filename)

      sc.stop()

  }

}