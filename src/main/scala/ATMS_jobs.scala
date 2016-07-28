

/**
  * Created by MJ on 2016. 5. 16..
  */

import com.typesafe.config.{Config, ConfigFactory}
import spark.jobserver._
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD
import edu.berkeley.cs.amplab.spark.indexedrdd.IndexedRDD._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{HashPartitioner, SparkConf, SparkContext}

import scala.collection.immutable.Iterable
import scala.collection.mutable.ArrayBuffer


trait ATMS_Job extends SparkJob with NamedRddSupport{

  override def validate(sc: SparkContext, config: Config): SparkJobValidation = { spark.jobserver.SparkJobValid }
}



object make_ATMS_RDD extends ATMS_Job {

  def runJob(sc: SparkContext, config: Config): Any = {

    var indexedHolds: RDD[(String, Set[Set[Int]])] = null
    var indexedAssumptions: RDD[(Long, String)] = null
    var indexedJustificands: RDD[(Long, (String, List[String]))] = null
    var indexedEnvs: RDD[(String, Long)] = null
    var indexedJustifiers: RDD[(String, Iterable[Long])] = null
    var indexedJustifieds: RDD[(String, Iterable[Long])] = null
    var nogoodListInt: Iterable[List[Int]] = Iterable.empty
//    var nogoods: RDD[String] = null
    var indexedLabels: RDD[(String, String)] = null
    var indexedNodes: RDD[(String, Long)] = null

    indexedHolds = sc.objectFile("output/holds")
    indexedHolds.count()

    indexedAssumptions = sc.objectFile("output/assumptions")
    indexedAssumptions.count()

    indexedJustificands = sc.objectFile("output/justs")
    indexedJustificands.count()

    indexedEnvs = sc.objectFile("output/envs")
    indexedEnvs.count()

    indexedJustifiers = sc.objectFile("output/justifiers")
    indexedJustifiers.count()

    indexedJustifieds = sc.objectFile("output/justifieds")
    indexedJustifieds.count()

    indexedLabels = sc.objectFile("output/labels")
    indexedLabels.count()

    indexedNodes = sc.objectFile("output/indexedNodes")
    indexedNodes.count()

    // for sharing RDD
    this.namedRdds.update("indexedHolds", indexedHolds) // Number of labels
    this.namedRdds.update("indexedAssumptions", indexedAssumptions) // number of assumption
    this.namedRdds.update("indexedJustificands", indexedJustificands)
    this.namedRdds.update("indexedEnvs", indexedEnvs) // number of env
    this.namedRdds.update("indexedJustifiers", indexedJustifiers)
    this.namedRdds.update("indexedJustifieds", indexedJustifieds)
    this.namedRdds.update("indexedLabels", indexedLabels)
    this.namedRdds.update("indexedNodes", indexedNodes)

//    this.namedRdds.update("nogoods", nogoods)


    val info = Map("indexedHolds" -> indexedHolds.count(),
                            "indexedAssumptions" -> indexedAssumptions.count(),
                            "indexedJustificands" -> indexedJustificands.count(),
                            "indexedEnvs" -> indexedEnvs.count(),
                            "indexedJustifiers" -> indexedJustifiers.count(),
                            "indexedJustifieds" -> indexedJustifieds.count(),
                            "indexedLabels" -> indexedLabels.count(),
                            "indexedNodes" -> indexedNodes.count(),
                            "inferredTriples" -> indexedJustificands.map{case (_, (n, _)) => n }.distinct().count()

    )

    info
  }
}



object ShowResource extends ATMS_Job {

  val prefToUriMap = Map("rdf" -> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs:" -> "<http://www.w3.org/2000/01/rdf-schema#",
    "owl:" -> "<http://www.w3.org/2002/07/owl#",
    "xbc:" -> "<http://xb.saltlux.com/schema/class/",
    "xbp:" -> "<http://xb.saltlux.com/schema/property/",
    "xbr:" ->"<http://xb.saltlux.com/resource/")

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




  def runJob(sc: SparkContext, config: Config): Any = {

    var keyword = config.getString("keyword")

    var result:Array[String] = null

    val indexedJustificands_tmp = this.namedRdds.get[(Long, (String, List[String]))]("indexedJustificands").get
    val indexedJustificands: IndexedRDD[Long, (String, List[String])] = IndexedRDD(indexedJustificands_tmp)

    val indexedLabels_tmp = this.namedRdds.get[(String, String)]("indexedLabels").get
    val indexedLabels: IndexedRDD[String, String] = IndexedRDD(indexedLabels_tmp)

    var infers: Array[(String, String, String)] = null
    var uris: Array[String] = null

    try {
      if (!keyword.equals("")) {
        keyword = Unicode.encode(keyword.trim())

        result = indexedLabels
          .filter { case (_, label) =>
            if (label != None) {
              label.toLowerCase.contains(keyword)
            } else {
              false
            }
          }
          .collect.map { case (uri, label) => uriToPrefix(uri) + " -> " + Unicode.decode(label) }
//          .foreach(println)
      } else {
        println("no input")
      }
    } catch {
      case e: Exception => println(e)
    }

    result

  }// runJob
}// showResource


object ShowTriples extends ATMS_Job {

  val prefToUriMap = Map("rdf" -> "<http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "rdfs:" -> "<http://www.w3.org/2000/01/rdf-schema#",
    "owl:" -> "<http://www.w3.org/2002/07/owl#",
    "xbc:" -> "<http://xb.saltlux.com/schema/class/",
    "xbp:" -> "<http://xb.saltlux.com/schema/property/",
    "xbr:" ->"<http://xb.saltlux.com/resource/")


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





  def runJob(sc: SparkContext, config: Config): Any = {

    var resourceId = config.getString("resourceId")

    val indexedJustificands_tmp = this.namedRdds.get[(Long, (String, List[String]))]("indexedJustificands").get
    val indexedJustificands: IndexedRDD[Long, (String, List[String])] = IndexedRDD(indexedJustificands_tmp)

    val indexedLabels_tmp = this.namedRdds.get[(String, String)]("indexedLabels").get
    val indexedLabels: IndexedRDD[String, String] = IndexedRDD(indexedLabels_tmp)


    var inferedTriples = ArrayBuffer[String]()

    try {
      if (!resourceId.trim().equals("")) {
        resourceId = prefixToUri(resourceId.trim())

        val infers: Array[(String, String, String)] =
          indexedJustificands.map { case (_, (node, _)) => node }
            .map { case n =>
              parseTriple(n)
            }
            .filter { case (s, p, o) => s.contains(resourceId) || o.contains(resourceId) }
            .collect()
            .distinct

        val uris = infers.flatMap { case (s, p, o) => List(s, p, o) }.distinct
        val labels = indexedLabels.multiget(uris)

        for ((s, p, o) <- infers) {
          val tmp = uriToPrefix(s) + " " + uriToPrefix(p) + " " + uriToPrefix(o) + " = " +
            Unicode.decode(labels.getOrElse(s, s)) + " " +
            Unicode.decode(labels.getOrElse(p, p)) + " " +
            Unicode.decode(labels.getOrElse(o, o))

          println("triple : " + tmp)

          inferedTriples += tmp
        }
      } else {
        println("No input!")
      }
    } catch {
      case e: Exception => println(e)
    }

    inferedTriples.toArray

  }
}





object Why extends ATMS_Job {

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



  def runJob(sc: SparkContext, config: Config): Any = {

    var nodename = config.getString("nodename")
    var nodenameWithPrefix = nodename

    val indexedHolds_tmp = this.namedRdds.get[(String, Set[Set[Int]])]("indexedHolds").get
    val indexedHolds: IndexedRDD[String, Set[Set[Int]]] = IndexedRDD(indexedHolds_tmp)

    val indexedAssumptions_tmp = this.namedRdds.get[(Long, String)]("indexedAssumptions").get
    val indexedAssumptions: IndexedRDD[Long, String] = IndexedRDD(indexedAssumptions_tmp)

    val indexedLabels_tmp = this.namedRdds.get[(String, String)]("indexedLabels").get
    val indexedLabels: IndexedRDD[String, String] = IndexedRDD(indexedLabels_tmp)

    val indexedJustifieds_tmp = this.namedRdds.get[(String, Iterable[Long])]("indexedJustifieds").get
    val indexedJustifieds: IndexedRDD[String, Iterable[Long]] = IndexedRDD(indexedJustifieds_tmp)

    val indexedJustificands_tmp = this.namedRdds.get[(Long, (String, List[String]))]("indexedJustificands").get
    val indexedJustificands: IndexedRDD[Long, (String, List[String])] = IndexedRDD(indexedJustificands_tmp)

    val indexedNodes_tmp = this.namedRdds.get[(String, Long)]("indexedNodes").get
    val indexedNodes: IndexedRDD[String, Long] = IndexedRDD(indexedNodes_tmp)

    val indexedEnvs_tmp = this.namedRdds.get[(String, Long)]("indexedEnvs").get
    val indexedEnvs: IndexedRDD[String, Long] = IndexedRDD(indexedEnvs_tmp)



    var con: String = null;
    var totalAnt = ArrayBuffer[(String, Array[String])]()

    try {

      val list = nodename.trim.split(" ")
      val subj = prefixToUri(list(0))
      val pred = prefixToUri(list(1))
      val obj = prefixToUri(list(2))
      nodename = subj + " " + pred + " " + obj

      val envs: Set[Set[Int]] = indexedHolds.get(nodename).get

      val localEnvIndices: Map[String, Long] = indexedEnvs.multiget(envs.map(e => e.mkString(",")).toArray)

      // assumId
      val assumpOfEnv: Array[Long] = envs.flatMap(e => e.map(a => a.toLong)).toArray
      // assumId, assumTriple
      val assumpTriples: Map[Int, (String, String, String)] =
        indexedAssumptions.multiget(assumpOfEnv)
          .map { case (assumId, assumTriple) => (assumId.toInt, parseTriple(assumTriple))}

      val uriLabels: Map[String, String] =
        indexedLabels.multiget(assumpTriples.flatMap { case (_, (s, p, o)) => List(s, p, o)}.toArray)


      con = Unicode.decode(uriLabels.getOrElse(subj, uriToPrefix(subj))) +
        " " + Unicode.decode(uriLabels.getOrElse(pred, uriToPrefix(pred))) +
        " " + Unicode.decode(uriLabels.getOrElse(obj, uriToPrefix(obj))) +
        " = " + nodenameWithPrefix;

      println("\nnode literals: " + nodenameWithPrefix +
        " = " + Unicode.decode(uriLabels.getOrElse(subj, uriToPrefix(subj))) +
        " " + Unicode.decode(uriLabels.getOrElse(pred, uriToPrefix(pred))) +
        " " + Unicode.decode(uriLabels.getOrElse(obj, uriToPrefix(obj))))
      println("-------------- Environments of Label ----------------")

      var count = 0

      for (env <- envs) {
        val envID: Long = localEnvIndices.get(env.mkString(",")).getOrElse(0).asInstanceOf[Long]
        println("Environment: " + envID)

        var ant = ArrayBuffer[String]()
        var arrOfEnv = ArrayBuffer[String]()

        for (a <- env) {
          val (s, p, o) = assumpTriples.get(a).get // URIs
          val decodedTriple = "{ " + a + " } = " + Unicode.decode(uriLabels.get(s).getOrElse(s) +
              " " + uriLabels.get(p).getOrElse(p) +
              " " + uriLabels.get(o).getOrElse(o))
          val output = decodedTriple + " = " +
                       uriToPrefix(s) + " " +
                       uriToPrefix(p) + " " +
                       uriToPrefix(o)

          ant += output

          // aggregate of Env
          arrOfEnv += a.toString

          println(output)
        }// for
        count += 1

        totalAnt += Tuple2("{ " + arrOfEnv.mkString(", ") + " } ", ant.toArray)

      }// for

      // for test
//      totalAnt += Tuple2("{ 100 }", Array("100 | \"Ken Mitsuishi\" \"거주\" \"후쿠오카 현\" = xbr:0000252677 xbp:livedIn xbr:0000549287"))

    } catch {
      case e: Exception => println(e)
    }


    // for drawing graph in web
    val graphInfo: Array[String] = getParents(sc,
                                              config,
                                              nodenameWithPrefix,
                                              indexedJustifieds,
                                              indexedJustificands,
                                              indexedLabels,
                                              indexedNodes,
                                              indexedHolds).toArray


    // send to the webBrowser
    val info = Map("consequent" -> con, "antecedent" -> totalAnt, "graphinfo" -> graphInfo)

    info


  } //runJob



  def getParents( sc: SparkContext,
                  config: Config,
                  node: String,
                  indexedJustifieds: IndexedRDD[String, Iterable[Long]],
                  indexedJustificands: IndexedRDD[Long, (String, List[String])],
                  indexedLabels: IndexedRDD[String, String],
                  indexedNodes: IndexedRDD[String, Long],
                  indexedHolds: IndexedRDD[String, Set[Set[Int]]]): ArrayBuffer[String] = {


    var graphInfo = ArrayBuffer[String]()

    var nodename = node

    val list = nodename.trim.split(" ")
    val subj = prefixToUri(list(0))
    val pred = prefixToUri(list(1))
    val obj = prefixToUri(list(2))
    nodename = subj + " " + pred + " " + obj

    var output: Map[Long, (String, List[String], Int)] = Map()
    var justs: Array[Long] = indexedJustifieds.get(nodename).get.toArray
    var finished = false
    var depth = 1

    try{

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
//        println("test7")
        depth += 1
      }

      val maxDepth = output.map { case (_, (_, _, depth)) => depth }.reduce((a,b) => math.max(a,b))
      // resource
      output = output.map { case (j, (n, an, depth)) => (j, (n, an, maxDepth - depth))}

      val nodenames: Array[String] = output.flatMap { case (_, (n, an, _)) => an :+ n }.toArray.distinct

      val labelDict: Map[String, String] = indexedLabels.multiget(nodenames.flatMap(nn => {
        val t = parseTriple(nn)
        Array(t._1, t._2, t._3)
      }))

      val nodeId: Map[String, Long] = indexedNodes.multiget(nodenames)

      for ((j, (n, an, depth)) <- output) {

        val (n_s, n_p, n_o) = parseTriple(n)

        println(j + " " + uriToPrefix(n_s) + " " + uriToPrefix(n_p) + " " + uriToPrefix(n_o) + ", " +
          an.map(a => parseTriple(a)).map{ case (s,p,o) => uriToPrefix(s) + " " + uriToPrefix(p) + " " + uriToPrefix(o)} + ", " + depth )


        val tmp_ant = an.map(triple => nodeId.get(triple).get)

        // (node id, triple of label)
        val zip_ant = tmp_ant.zip(an)

        graphInfo += j + " " +
                        "[" + nodeId.get(n).get + "]" + Unicode.decode(labelDict.get(n_s).getOrElse(n_s)) + " " +
          Unicode.decode(labelDict.get(n_p).getOrElse(n_p)) + " " +
          Unicode.decode(labelDict.get(n_o).getOrElse(n_o)) + ", " +
          zip_ant.map{case (idx, triple) => (idx, parseTriple(triple))}
                 .map{ case (idx, (s,p,o)) => "[" + idx + "]" +
                   Unicode.decode(labelDict.get(s).getOrElse(s)) + " " +
                   Unicode.decode(labelDict.get(p).getOrElse(p)) + " " +
                   Unicode.decode(labelDict.get(o).getOrElse(o)) } + ", " + depth

        println(j + " " + nodeId.get(n).get + ", " +
          an.map{ triple => nodeId.get(triple).get} + " " + depth )

      }// for

      // fot test
      println("graph : " + graphInfo);

    }// try
    catch {
      case e: Exception => {
        graphInfo.clear()
        graphInfo += "Not Exist"
        println("Occured Exception")
      }
    }

    return graphInfo

  }// getParents

}





object Retract extends ATMS_Job {


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



  def retractInconsistentAssump(assumption: String,
                                indexedAssumptions: IndexedRDD[Long, String],
                                indexedHolds: IndexedRDD[String, Set[Set[Int]]],
                                nogoods: List[String]) = {

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
    val nogoods1 = nogoods :+ assumption
    val indexedHolds1 = indexedHolds.multiput(newLabels)

    (nogoods1, indexedHolds1)
  }



  def runJob(sc: SparkContext, config: Config): Any = {

    var assumption: String = config.getString("assumption")

    val indexedAssumptions_tmp = this.namedRdds.get[(Long, String)]("indexedAssumptions").get
    val indexedAssumptions: IndexedRDD[Long, String] = IndexedRDD(indexedAssumptions_tmp)

    val indexedHolds_tmp = this.namedRdds.get[(String, Set[Set[Int]])]("indexedHolds").get
    val indexedHolds: IndexedRDD[String, Set[Set[Int]]] = IndexedRDD(indexedHolds_tmp)

    val nogoods: List[String] = this.namedRdds.get[String]("nogoods") match {
      case Some(v) =>
        v.collect().toList
      case None => {
        List.empty
      }
    }


    try {

      val list = assumption.trim.split(" ")
      val subj = prefixToUri(list(0))
      val pred = prefixToUri(list(1))
      val obj = prefixToUri(list(2))
      assumption = subj + " " + pred + " " + obj

      val (nogoods1, indexedHolds1) = retractInconsistentAssump(assumption, indexedAssumptions, indexedHolds, nogoods)

      // convert IndexedRDD to RDD
      val holds1RDD = indexedHolds1.map( x => (x._1, x._2))
      val nogoods1RDD = sc.parallelize(nogoods1)

      // update RDD
      this.namedRdds.update("indexedHolds", holds1RDD) // Number of labels
      this.namedRdds.update("nogoods", nogoods1RDD) // Number of labels

    } catch {
      case e: Exception => println(e)
    }


    val result = "Success Retract!!"

    result

  }//runJob
}// Retract





