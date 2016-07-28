import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}
import org.apache.spark.util.collection.CompactBuffer

import scala.collection.immutable.Iterable
import scala.collection.immutable.List.empty

/**
  * Created by batselemjagvaral1 on 2/13/16.
  */

object Atms {

  var assumpCount = 0
  var justCount = 0

  var sc: SparkContext = null

  val vocabDict = Map("<http://www.w3.org/1999/02/22-rdf-syntax-ns#" -> "rdf",
    "<http://www.w3.org/2000/01/rdf-schema#" -> "rdfs:",
    "<http://www.w3.org/2002/07/owl#" -> "owl:",
    "<http://xb.saltlux.com/schema/class/" ->  "xbc:",
    "<http://xb.saltlux.com/schema/property/" -> "xbp:",
    "<http://xb.saltlux.com/resource/" -> "xbr:")

  def uriToPrefix(uri: String) = {
    var fixedUri = uri
    for ((k, v) <- vocabDict) {
      if (uri.contains(k)) {
        fixedUri = uri.replaceAll(k, v)
        fixedUri = fixedUri.substring(0, fixedUri.length - 1)
      }
    }
    fixedUri
  }
  
  val prefToUriMap = Map("rdf" -> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs:" -> "<http://www.w3.org/2000/01/rdf-schema#",
    "owl:" -> "<http://www.w3.org/2002/07/owl#",
    "xbc:" -> "<http://xb.saltlux.com/schema/class/",
    "xbp:" -> "<http://xb.saltlux.com/schema/property/",
    "xbr:" ->"<http://xb.saltlux.com/resource/")


  def prefixToUri(pref: String) = {
    var uri = pref
    for ((k, v) <- prefToUriMap) {
      if (uri.contains(k)) {
        uri = uri.replaceFirst(k, v)
        uri = uri.substring(0, uri.length) + ">"
      }
    }
    uri
  }
  
  var indexedHolds: IndexedRDD[String, Set[Set[Int]]] = null
  var indexedAssumptions: IndexedRDD[Long, String] = null
  var indexedJustificands: IndexedRDD[Long, (String, List[String])] = null
  var indexedEnvs: IndexedRDD[String, Long] = null

  var indexedNodes: IndexedRDD[String, Long] = null

  var indexedJustifiers: IndexedRDD[String, Iterable[Long]] = null
  var indexedJustifieds: IndexedRDD[String, Iterable[Long]] = null
  var nogoodListInt: Iterable[List[Int]] = Iterable.empty
  var nogoods: List[String] = List.empty
  var indexedLabels: IndexedRDD[String, String] = null

  def crossUnion( label1: Set[Set[Int]], label2: Set[Set[Int]] ): Set[Set[Int]] = {
    var list: Set[Set[Int]] = Set.empty

    for (e1 <- label1; e2 <- label2) {
      val e = e1.union(e2)
      if (!list.contains(e)) list = list + e
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

  def retractInconsistentAssump(assumption: String) = {

    /*
    indexedEnvs = indexedEnvs.multiput(envs.map(e => (e.mkString(","), false)).toMap)
    */

    val aId = indexedAssumptions.filter { case (id, a) => a.equals(assumption)}.take(1).head._1.toInt

    val retractNodeNames: Array[String] = indexedHolds.mapPartitions(iter => {

      for {
        (node, label) <- iter
        if (!label.forall(e => !e.contains(aId)) && !node.equals("false"))
      } yield node
    }, true).collect()

    val retractNodes: Map[String, Set[Set[Int]]] = indexedHolds.multiget(retractNodeNames)

    val newLabels: Map[String, Set[Set[Int]]] = retractNodes.map { case (node, label) =>
      (node, label.filter(e => e.forall(a => !a.equals(aId))))
    }
    nogoods = nogoods :+ assumption
    indexedHolds = indexedHolds.multiput(newLabels)
  }

  def undoAssumpRetraction(assumption: String) = {
    val assumptionId: Long = indexedAssumptions.filter { case (id, a) => a.equals(assumption)}.take(1).head._1

    indexedHolds = indexedHolds.put(assumption, Set(Set(assumptionId.toInt)))
    val justIds: Iterable[Long] = indexedJustifiers.get(assumption).get
    println("justified : " + justIds.size)
    for (j <- justIds) {
      nodeUpdateEnv(j)
    }
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

      val newEnvList = ((oldEnvList.get ++ envList))

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

  def unicode(uni: String) = {

  }

  def replaceResource(triple: (String, String, String)) = {

    import java.text.Normalizer

    var s = if (triple._1.startsWith("<")) {
      indexedLabels.get(triple._1).getOrElse(triple._1)
    } else triple._1

    var p = if (triple._2.startsWith("<")) {
      indexedLabels.get(triple._2).getOrElse(triple._2)
    } else triple._2

    var o = if (triple._3.startsWith("<")) {
      indexedLabels.get(triple._3).getOrElse(triple._3)
    } else triple._3

    s + " " + p + " " + o

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
    val pred = triple0.substring(0, triple0.indexOf(' '))

    val triple1 = triple0.substring(pred.length + 1)

    // Get object
    val obj = if (triple1.startsWith("<")) {
      triple1.substring(0, triple1.indexOf('>') + 1)
    }
    else if (triple1.startsWith("\"")) {
      triple1.substring(0, triple1.substring(1).indexOf('\"') + 2)
    }
    else {
      if (triple1.endsWith(".")) triple1.substring(0, triple1.length - 2)
      else triple1.trim
    }

    (subj, pred, obj)
  }

  def printJustificands(just: (Long, List[String])) = {
    println("j:" + just._1)
    for (j <- just._2) {

    }
    for (assum <- just._2) {
      val (s, p, o) = parseTriple(assum)

      println("\t" + uriToPrefix(s) + " " + uriToPrefix(p) + " " + uriToPrefix(o))
    }
  }


  def main(args: Array[String]): Unit = {

    //System.setProperty("hadoop.home.dir", "c:\\winutil\\")

    sc = new SparkContext({
      new SparkConf()
        .setAppName("ATMS")
        .setMaster("local[*]")
        .set("spark.io.compression.codec", "lz4")
        .set("spark.broadcast.compress", "true")
        .set("spawhyrk.locality.wait", "10000")
        .set("spark.shuffle.compress", "true")
        .set("spark.rdd.compress", "true")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    })
    setLogLevel(Level.ERROR)

    val startTime = System.currentTimeMillis()
    //indexedLabels = IndexedRDD ( sc.objectFile("atms/labels") )
    indexedHolds = IndexedRDD( sc.objectFile("/Users/MJ/Desktop/atms/spark-jobserver-jobserver-0.6.1-spark-1.5.2/job-server/output/holds") )
    indexedHolds.count()
    indexedAssumptions = IndexedRDD( sc.objectFile("/Users/MJ/Desktop/atms/spark-jobserver-jobserver-0.6.1-spark-1.5.2/job-server/output/assumptions") )
    indexedAssumptions.count()

    indexedJustificands = IndexedRDD( sc.objectFile("/Users/MJ/Desktop/atms/spark-jobserver-jobserver-0.6.1-spark-1.5.2/job-server/output/justs") )
    indexedJustificands.count()
    indexedEnvs = IndexedRDD( sc.objectFile("/Users/MJ/Desktop/atms/spark-jobserver-jobserver-0.6.1-spark-1.5.2/job-server/output/envs") )
    indexedEnvs.count()
    indexedJustifiers = IndexedRDD( sc.objectFile("/Users/MJ/Desktop/atms/spark-jobserver-jobserver-0.6.1-spark-1.5.2/job-server/output/justifiers") )
    indexedJustifiers.count()
    indexedJustifieds = IndexedRDD( sc.objectFile("/Users/MJ/Desktop/atms/spark-jobserver-jobserver-0.6.1-spark-1.5.2/job-server/output/justifieds"))
    indexedLabels = IndexedRDD( sc.objectFile("/Users/MJ/Desktop/atms/spark-jobserver-jobserver-0.6.1-spark-1.5.2/job-server/output/labels"))
    indexedNodes = IndexedRDD( sc.objectFile("/Users/MJ/Desktop/atms/spark-jobserver-jobserver-0.6.1-spark-1.5.2/job-server/output/indexedNodes"))
    indexedLabels.take(10).foreach(println)

    val endTime = System.currentTimeMillis()


    println("assumption count : " + indexedAssumptions.count().toInt)

    //Map(2 -> List((2,b), (2,bb)), 1 -> List((1,a), (1,aa)), 3 -> List((3,c)))

    println("time :" + (endTime - startTime)/1000 + " sec")
    println("Atms is created. Please type your command!")
    try {
      var ok = true
      while (ok) {
        print(">> ")
        val line = readLine()

        ok = line != null
        if (ok) {
          line match {
            case "add-just" =>
              print("antecedents:")
              val antecedents = readLine().split(",").map(x => x.trim())
              print("conclusion:")
              val conclusion = readLine()
              val time0 = System.currentTimeMillis()

              atmsAddJust(antecedents, conclusion)

              println("runtime : " + (System.currentTimeMillis() - time0) / 1000.0 + " sec")

            case "why" =>
              print("node name:")
              var nodename = readLine()
              val time0 = System.currentTimeMillis()

              try {

                val list = nodename.trim.split(" ")
                val subj = prefixToUri(list(0))
                val pred = prefixToUri(list(1))
                val obj = prefixToUri(list(2))
                nodename = subj + " " + pred + " " + obj

                val envs: Set[Set[Int]] = indexedHolds.get(nodename).get
                println(envs)

                val localEnvIndices: Map[String, Long] = indexedEnvs.multiget(envs.map(e => e.mkString(",")).toArray)

                // assumId
                val assumpOfEnv: Array[Long] = envs.flatMap(e => e.map(a => a.toLong)).toArray
                // assumId, assumTriple
                val assumpTriples: Map[Int, (String, String, String)] =
                  indexedAssumptions.multiget(assumpOfEnv)
                    .map { case (assumId, assumTriple) => (assumId.toInt, parseTriple(assumTriple))}

                val uriLabels: Map[String, String] =
                  indexedLabels.multiget(assumpTriples.flatMap { case (_, (s, p, o)) => List(s, p, o)}.toArray)

                println("\nnode literals: " + Unicode.decode(uriLabels.getOrElse(subj, subj)) +
                  " " + Unicode.decode(uriLabels.getOrElse(pred, pred)) +
                  " " + Unicode.decode(uriLabels.getOrElse(obj, obj)))
                println("-------------- Environments of Label ----------------")

                var count = 0
                for (env <- envs) {
                  println("Env" + localEnvIndices.get(env.mkString(",")).getOrElse(0))
                  for (a <- env) {
                    val (s, p, o) = assumpTriples.get(a).get // URIs
                    val decodedTriple = a + " : " + Unicode.decode(uriLabels.get(s).getOrElse(s) +
                        " " + uriLabels.get(p).getOrElse(p) +
                        " " + uriLabels.get(o).getOrElse(o))
                    val output = "   " + decodedTriple + " = " + uriToPrefix(s) + " " + uriToPrefix(p) + " " + uriToPrefix(o)
                    println(output)
                  }
                  count += 1
                }

              } catch {
                case e: Exception => println(e)
              }
              println("runtime : " + (System.currentTimeMillis() - time0) / 1000.0 + " sec")

            case "retract" =>
              print("assumption name:")
              var assumption = readLine()
              val time0 = System.currentTimeMillis()

              try {

                val list = assumption.trim.split(" ")
                val subj = prefixToUri(list(0))
                val pred = prefixToUri(list(1))
                val obj = prefixToUri(list(2))
                assumption = subj + " " + pred + " " + obj

                retractInconsistentAssump(assumption)

              } catch {
                case e: Exception => println(e)
              }
              println("runtime : " + (System.currentTimeMillis() - time0) / 1000.0 + " sec")

            case "assert" =>
              print("assumption name:")
              val time0 = System.currentTimeMillis()

              var assumption = readLine()
              try {
                val list = assumption.trim.split(" ")
                val subj = prefixToUri(list(0))
                val pred = prefixToUri(list(1))
                val obj = prefixToUri(list(2))
                assumption = subj + " " + pred + " " + obj
                undoAssumpRetraction(assumption)
              } catch {
                case e: Exception => println(e)
              }
              println("runtime : " + (System.currentTimeMillis() - time0) / 1000.0 + " sec")


            case "show-belief" =>

              print("From-To:")
              val fromTo = readLine().split("-").toList.map(x => x.trim)
              print("How many?:")
              val numNodes = readLine().trim().toInt
              val time0 = System.currentTimeMillis()

              val from = fromTo(0).toInt
              val to = fromTo(1).toInt

              indexedHolds.filter { case (n, l) => from < l.size && l.size < to }
                .take(numNodes)
                .map { case (n, l) => (n, l.size) }
                .foreach(println)

              println("runtime : " + (System.currentTimeMillis() - time0) / 1000.0 + " sec")
            case "show-nogood" =>
              nogoods.foreach(println)
            case "create_assumption" =>
              print("your assumption:")
              val assump = readLine()
              declareAssumption(assump)
            case "show-in" =>
              indexedHolds.collect.foreach(println)
              //indexedHolds.map { case (n, l) => (l.size, n)}.collect.foreach(println)
            case "show-out" =>
              indexedHolds.filter(x => x._2.isEmpty).take(10).map(x => x._1).foreach(println)
            case "exit" => ok = false
              sc.stop()

            case "search" =>
              print("your keyword:")
              var keyword = readLine()

              try {
                if (!keyword.trim().equals("")) {
                  keyword = prefixToUri(keyword.trim())

                  val infers: Array[(String, String, String)] =
                    indexedJustificands.map { case (_, (node, _)) => node }
                      .map { case n =>
                        parseTriple(n)
                      }
                      .filter { case (s, p, o) => s.contains(keyword) || o.contains(keyword) }
                      .collect()
                      .distinct

                  val uris = infers.flatMap { case (s, p, o) => List(s, p, o) }.distinct
                  val labels = indexedLabels.multiget(uris)

                  for ((s, p, o) <- infers) {
                    println(uriToPrefix(s) + " " + uriToPrefix(p) + " " + uriToPrefix(o) + " = " +
                      Unicode.decode(labels.getOrElse(s, s)) + " " +
                      Unicode.decode(labels.getOrElse(p, p)) + " " +
                      Unicode.decode(labels.getOrElse(o, o)))
                  }
                } else {
                  println("No input!")
                }
              } catch {
                case e: Exception => println(e)
              }

            case "get-uri" =>

              try {
                print("keyword:")
                var keyword = readLine()
                if (!keyword.equals("")) {
                  keyword = Unicode.encode(keyword.trim())

                  indexedLabels
                    .filter { case (_, label) =>
                      if (label != None) {
                        label.toLowerCase.contains(keyword)
                      } else {
                        false
                      }
                    }
                    .collect.map { case (uri, label) => uriToPrefix(uri) + " : " + Unicode.decode(label) }
                    .foreach(println)
                } else {
                  println("no input")
                }
              } catch {
                case e: Exception => println(e)
              }
            case "get-instances" =>
              try {
                print("class name:")
                val classname = readLine()
                if (!classname.equals("")) {
                  val result = indexedHolds
                    .filter { case (node, _) =>
                      node.contains("type") && parseTriple(node)._3.contains(classname)
                    }
                    .map { case (node, _) => node }
                    .map(node => (node.substring(0, node.indexOf(">") + 1), node))
                    .leftOuterJoin(indexedLabels)
                    .map { case (uri, (triple, optUri)) => (triple, Unicode.decode(optUri.getOrElse(uri))) }
                    .collect()
                  var i = 1
                  for (tuple <- result) {
                    val (s, p, o) = parseTriple(tuple._1)
                    println(i + " : " + uriToPrefix(s) + " " + p + " " + uriToPrefix(o) + " : " + tuple._2)
                    i += 1
                  }
                } else {
                  println("no input!")
                }

              } catch {
                case e: Exception => println(e)
              }

            case "get-parents" =>
              print("node name:")
              var nodename = readLine()

                val list = nodename.trim.split(" ")
                val subj = prefixToUri(list(0))
                val pred = prefixToUri(list(1))
                val obj = prefixToUri(list(2))
                nodename = subj + " " + pred + " " + obj

                val time0 = System.currentTimeMillis()
                var output: Map[Long, (String, List[String], Int)] = Map()
                var justs: Array[Long] = indexedJustifieds.get(nodename).get.toArray
                justs.foreach(println)

                var finished = false
                var depth = 1


                while (!finished) {
                  var justList = indexedJustificands.multiget(justs)
                  var antecedants = justList.values.toArray.flatMap(x => x._2).distinct


                  val inoutAntecedants = indexedHolds.multiget(antecedants).map { case (n, l) => (n, !l.isEmpty) }

                  justList = justList.filter { case (_, (_, an)) => an.forall(a => inoutAntecedants.get(a).get) }

                  antecedants = justList.values.toArray.flatMap(x => x._2).distinct

                  output = output ++ justList.map { case (j, (n, an)) => (j, (n, an, depth)) }

                  val result = indexedJustifieds.multiget(antecedants)
                  println(justList)
                  val temp = result.toList.flatMap(x => x._2.toList)

                  if (!result.isEmpty) {
                    /*
                    var flattened: Array[Long] = Array()
                    for (ids <- result.values) {
                      for (i <- ids) {
                        flattened = flattened :+ i
                      }
                    }
                    */
                    justs = temp.distinct.toArray
                  } else finished = true
                  println("test7")
                  depth += 1
                }


                val maxDepth = output.map { case (_, (_, _, depth)) => depth }.reduce((a, b) => math.max(a, b))
                // resource
                output = output.map { case (j, (n, an, depth)) => (j, (n, an, maxDepth - depth)) }

                val nodenames: Array[String] = output.flatMap { case (_, (n, an, _)) => an :+ n }.toArray.distinct

                val labelDict: Map[String, String] = indexedLabels.multiget(nodenames.flatMap(nn => {
                  val t = parseTriple(nn)
                  Array(t._1, t._2, t._3)
                }))

                val nodeId: Map[String, Long] = indexedNodes.multiget(nodenames)

                for ((j, (n, an, depth)) <- output) {

                  val (n_s, n_p, n_o) = parseTriple(n)

                  println(j + " " + uriToPrefix(n_s) + " " + uriToPrefix(n_p) + " " + uriToPrefix(n_o) + ", " +
                    an.map(a => parseTriple(a)).map { case (s, p, o) => uriToPrefix(s) + " " + uriToPrefix(p) + " " + uriToPrefix(o) } + ", " + depth)

                  val tmp_ant = an.map(triple => nodeId.get(triple).get)

                  // (node id, triple of label)
                  val zip_ant = tmp_ant.zip(an)

                  val graphInfo = j + " " +
                    "[" + nodeId.get(n).get + "]" + Unicode.decode(labelDict.get(n_s).getOrElse(n_s)) + " " +
                    Unicode.decode(labelDict.get(n_p).getOrElse(n_p)) + " " +
                    Unicode.decode(labelDict.get(n_o).getOrElse(n_o)) + ", " +
                    zip_ant.map { case (idx, triple) => (idx, parseTriple(triple)) }
                      .map { case (idx, (s, p, o)) => "[" + idx + "]" +
                        Unicode.decode(labelDict.get(s).getOrElse(s)) + " " +
                        Unicode.decode(labelDict.get(p).getOrElse(p)) + " " +
                        Unicode.decode(labelDict.get(o).getOrElse(o))
                      } + ", " + depth

                  println(graphInfo)


                  println(j + " " + nodeId.get(n).get + ", " +
                    an.map { triple => nodeId.get(triple).get } + " " + depth)

                }



            case "get-direct-parents" =>
              print("node name:")
              var nodename = readLine()
              val time0 = System.currentTimeMillis()

              try {

                val list = nodename.trim.split(" ")
                val subj = prefixToUri(list(0))
                val pred = prefixToUri(list(1))
                val obj = prefixToUri(list(2))
                nodename = subj + " " + pred + " " + obj

                val justIds = indexedJustifieds.get(nodename).get.toArray
                if (justIds.size > 20) new throws("The number of justifications is too large!")

                if (!justIds.isEmpty) {

                  val justificands = indexedJustificands
                    .multiget(justIds)
                    .map { case (jId, (_, pars)) => (jId, pars) }

                  val uris = justificands.flatMap {
                    case (_, ants) => ants.flatMap { case a => val (s, p, o) = parseTriple(a); List(s, p, o) }
                  }.toArray.distinct

                  val assumLabels = indexedLabels.multiget(uris)

                  println("\nnode literals: " + Unicode.decode( assumLabels.getOrElse(subj, subj) ) +
                    " " + list(1) + " " + Unicode.decode( assumLabels.getOrElse(obj, obj)) )
                  println("------------- Direct Parent Justifications ------------")

                  for ((j, assumps) <- justificands) {
                    println("Justification: " + j)
                    for (a <- assumps) {
                      val (s, p, o) = parseTriple(a)
                      val decodedTriple = Unicode.decode(assumLabels.getOrElse(s, s) +
                        " " + assumLabels.getOrElse(p, p) +
                        " " + assumLabels.getOrElse(o, o))

                      println("  " + decodedTriple + " = " + uriToPrefix(s) + " " + uriToPrefix(p) + " " + uriToPrefix(o))
                    }
                  }
                } else {
                  println("No parent")
                }

              } catch {
                case e: Exception => println(e)
              }
              println("runtime : " + (System.currentTimeMillis() - time0) / 1000.0 + " sec")

            case _ => println("command is not found!")
          }
        }
      }
    } catch {
      case e: Exception => print(e)
    } finally {
      sc.stop()
    }
  }

}
