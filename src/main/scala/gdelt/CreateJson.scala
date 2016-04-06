package gdelt

import java.io._
import scala.xml._
import scala.xml.NodeSeq.seqToNodeSeq
import graph.EntityGraph

object createjson extends App {
  // Utilities.dumpXmlToJson("/home/saleem/work/learn/mine/gdelt-gkg-scala/graph.xml", "graph.json")

//  val personGraph = new EntityGraph("personGraph-neo4j")
//  personGraph.printNeighbours
//  personGraph.close
//  val orgGraph = new EntityGraph("orgGraph-neo4j")
//  orgGraph.printNeighbours
//  orgGraph.close
  val themeGraph = new EntityGraph("themeGraph-neo4j")
  themeGraph.printNeighbours
  themeGraph.close
}
