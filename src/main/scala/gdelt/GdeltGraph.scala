package gdelt

import com.mongodb.casbah.Imports._
import scala.collection.mutable

object gdeltgrah extends App {
  println("Hello")

  val CUTOFF_NODE = 1
  val CUTOFF_EDGE = 1
  val FILTER_THEME = "ENV_OIL"
  val FILTER_LOCATION = "Russia"

  // 1=PERSONNAMES, 2=ORGANIZATIONNAMES, 3=THEMES
  val NAMETYPE = 1

  // aggregate of tone, per article for this entity
  val toneMap = mutable.Map[String, Double]()

  // number of articles in which this entity appeared
  val articleCounts = mutable.Map[String, Int]()

  // node id of this entity
  val nodeIds = mutable.Map[String, Int]()

  // number of records in which this entity appeared
  val recordCounts = mutable.Map[String, Int]()

  println("Fetch entries")
  
  val gkgCollection = Utilities.getCollection
  val allCount = gkgCollection.count()
  val allEntries = gkgCollection.find()

  val filterTheme = FILTER_THEME.toLowerCase
  val filterLocation = FILTER_LOCATION.toLowerCase

  val coOccurenceMap = mutable.Map[Pair[String, String], Int]()

  println("Total Entries: " + allCount)
  var entryNumber = 0
  var nodeId = 0
  for (e <- allEntries) {
    val record = new GkgRecord(e)
    print("" + record.date + "  -- " + entryNumber + "" + "\r")
    entryNumber += 1

    // if this entry matches the theme
    // and also matches the filter location

    // println(record.sourceurls)
    if (record.themes.exists(_.toLowerCase.contains(filterTheme)) &&
      record.locations.exists(_.toLowerCase.contains(filterLocation))) {
      val elements = NAMETYPE match {
        case 1 => record.persons
        case 2 => record.organizations
        case 3 => record.themes
      }

      // aggregate for the key ( all values in the list )
      elements foreach { name =>
        articleCounts(name) = articleCounts.getOrElse(name, 0) + record.numarts
        toneMap(name) = toneMap.getOrElse(name, 0.0) + (record.tone * record.numarts)
        recordCounts(name) = recordCounts.getOrElse(name, 0) + 1
        nodeIds.getOrElseUpdate(name, { nodeId += 1; nodeId })
      }

      val key = elements.sorted
      // calculate local co-occurence
      val partialCooccurence = (for (
        i <- (0 until key.length);
        j <- ((i + 1) until key.length)
      ) yield (key(i), key(j)) -> 1) toMap

      partialCooccurence foreach { elem =>
        val k = elem._1
        val v = elem._2
        coOccurenceMap(k) = coOccurenceMap.getOrElse(k, 0) + v
      }
    }
  }

  // fetch top co-occuring entries by cooccurence measure
  ((coOccurenceMap toList) sortBy { a => a._2
  }).reverse take (40) foreach println

  val maxWeight = coOccurenceMap.maxBy(_._2)._2

  // print nodes
  // print all the visited nodes
  val vertices = mutable.ArrayBuffer[xml.Elem]()
  for (n <- nodeIds) {
    val nodeName = n._1
    val nodeId = n._2

    // create XML tag for this vertex
    val vertexNode = <node id={ nodeId.toString } name={ nodeName }/>
    vertices += vertexNode
    // println(edgeNode)
  }

  // print edges
  // filter by cutoff value
  val edges = mutable.ArrayBuffer[xml.Elem]()
  for ((edge, i) <- (coOccurenceMap filter (p => p._2 >= CUTOFF_EDGE)).view.zipWithIndex) {
    val (sourceName, targetName) = edge._1
    val (sourceId, targetId) = (nodeIds(sourceName), nodeIds(targetName))

    // get normalized weight
    val edgeWeight = edge._2
    val normalizedWeight = edgeWeight / (maxWeight + 0.0)

    // create XML tag for this edge
    val edgeXml = <edge id={ i.toString } source={ sourceId.toString } target={ targetId.toString } weight={ normalizedWeight.toString }/>
    // println(edgeNode)
    edges += edgeXml
  }

  val graph =
    <gexf xmlns="http://www.gexf.net/1.1draft" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xsi:schemaLocation="http://www.gexf.net/1.1draft http://www.gexf.net/1.1draft/gexf.xsd" version="1.1">
      <graph mode="static" defaultedgetype="undirected">
        <edges>{ edges }</edges>
        <nodes>{ vertices }</nodes>
      </graph>
    </gexf>

  // val printer = new xml.PrettyPrinter(width = 100, step = 4)
  // val str = printer.formatNodes(graph)
  // println(graph)
  xml.XML.save("graph.xml", graph)

  // TODO:
  // Find unique combinations of entity groups 
}
