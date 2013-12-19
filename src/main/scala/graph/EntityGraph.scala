package graph

import org.neo4j.graphdb.Direction
import org.neo4j.graphdb.GraphDatabaseService
import org.neo4j.graphdb.Node
import org.neo4j.graphdb.Path
import org.neo4j.graphdb.Relationship
import org.neo4j.graphdb.RelationshipType
import org.neo4j.graphdb.Transaction
import org.neo4j.graphdb.factory.GraphDatabaseFactory
import org.neo4j.graphdb.traversal.Evaluators
import org.neo4j.graphdb.traversal.TraversalDescription
import org.neo4j.graphdb.traversal.Traverser
import org.neo4j.kernel.Traversal
import scala.sys.ShutdownHookThread
import collection.JavaConversions._
import java.io.File
import scala.collection.mutable
import org.neo4j.graphdb.factory.GraphDatabaseSettings
import gdelt.Utilities

class EntityGraph(val graphLocation: String) {
  private def neo4jStoreDir = graphLocation

  // Utilities.deleteFileOrDirectory(new File(neo4jStoreDir))

  val graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(neo4jStoreDir)
    .setConfig(GraphDatabaseSettings.node_keys_indexable, "name")
    .setConfig(GraphDatabaseSettings.node_auto_indexing, "true")
    .newGraphDatabase()

  // register shutdown hook thread
  ShutdownHookThread { graphDb.shutdown() }

  val COOCCUR_RT = new RelationshipType {
    def name(): String = "COOCCUR"
  }

  def withinTransaction(block: => Unit) = {
    val tx = graphDb.beginTx
    block
    tx.success()
    tx.finish()
  }

  private def fetchNode(nodeName: String): Option[Node] = {
    val autoIndex = graphDb.index().getNodeAutoIndexer().getAutoIndex()
    val node = autoIndex.get("name", nodeName).getSingle()
    if (node == null) { // we create a new node
      None
    } else Some(node)
  }

  private def createOrFetchNode(nodeName: String) = {
    // val autoIndex = graphDb.index().getNodeAutoIndexer().getAutoIndex()
    // val node = autoIndex.get("name", nodeName).getSingle()
    val node = fetchNode(nodeName)
    node match {
      case Some(n) => n
      case None =>
        val node = graphDb.createNode()
        node.setProperty("name", nodeName)
        node
    }
    //    if (node == null) { // we create a new node
    //      val node = graphDb.createNode()
    //      node.setProperty("name", nodeName)
    //      node
    //    } else node
  }

  private def getCoOccurenceEdge(sourceNode: Node, targetNode: Node): Relationship = {
    // only those cooccurence relationships which connect source and target nodes
    val relationships = sourceNode.getRelationships(COOCCUR_RT).filter {
      r => r.getEndNode().getId() == targetNode.getId()
    }.take(1).toList

    if (relationships.size > 0) relationships(0)
    else sourceNode.createRelationshipTo(targetNode, COOCCUR_RT)
  }

  private def getCoOccurenceCount(relCoOccur: Relationship) = {
    try {
      val c = if (relCoOccur.hasProperty("COUNT")) relCoOccur.getProperty("COUNT") else 0
      c.asInstanceOf[Int]
    } catch { case x: Throwable => 0 }
  }

  private def setCoOccurenceCount(relCoOccur: Relationship, newCount: Int): Int = {
    relCoOccur.setProperty("COUNT", newCount); newCount
  }

  private def getPartialCooccurence(elements: Seq[String]) = {
    /*
     * For List("A", "B", "C")
     * return
     * Map(("A", "B") -> 1, ("A", "C") -> 1, ("B", "C") -> 1)
     */
    (for (
      i <- (0 until elements.length);
      j <- ((i + 1) until elements.length)
    ) yield (elements(i), elements(j)) -> 1) toMap
  }

  def processRecord(r: Seq[String]) = {
    val partialCooccurence = getPartialCooccurence(r.sorted)
    partialCooccurence foreach { elem =>
      val nodes = elem._1
      val value = elem._2
      val (sourceNodeName, targetNodeName) = elem._1
      val sourceNode = createOrFetchNode(sourceNodeName)
      val targetNode = createOrFetchNode(targetNodeName)
      val relCoOccur = getCoOccurenceEdge(sourceNode, targetNode)
      val count: Int = getCoOccurenceCount(relCoOccur)
      val newCount = setCoOccurenceCount(relCoOccur, count + value)
      println(s"$sourceNodeName $targetNodeName $newCount")
    }
  }

  // treat edges as undirected / symmetric matrix
  def getCoOccurence(a: String, b: String): Int = {
    val nodeA = fetchNode(a)
    nodeA match {
      case Some(nodeA) => {
        val nodeB = fetchNode(b)
        nodeB match {
          case Some(nodeB) => {
            val relCoOccur = getCoOccurenceEdge(nodeA, nodeB)
            getCoOccurenceCount(relCoOccur)
          }
          case None => 0
        }
      }
      case None => 0
    }
  }

  def printNeighbours = {
    withinTransaction {
      for (n <- graphDb.getAllNodes() if n.hasProperty("name")) {
        val myName = n.getProperty("name")
        val entries = n.getRelationships().filter { r =>
          r.hasProperty("COUNT")
        }.map { r =>
          val o = r.getOtherNode(n)
          val count = getCoOccurenceCount(r)
          (o.getProperty("name"), count)
        }
        println(s"$myName => $entries")
      }
    }
  }

  def print() = {
    withinTransaction {

      def getFriends(person: Node): Traverser = {
        val td = Traversal.description().breadthFirst()
          .relationships(COOCCUR_RT, Direction.BOTH)
          .evaluator(Evaluators.excludeStartPosition())
        td.traverse(person)
      }

      for (n <- graphDb.getAllNodes() if n.hasProperty("name")) {

        // n.get

        val sourceName = n.getProperty("name")
        var numberOfFriends = 0
        val friendsTraverser = getFriends(n);
        for (friendPath <- friendsTraverser) {
          val depth = friendPath.length()
          println(s"From $sourceName at depth $depth => ${friendPath.endNode().getProperty("name")}")
          numberOfFriends += 1
        }
        println("Number of friends found: " + numberOfFriends)
      }
    }
  }
  
  def close = {
    graphDb.shutdown()
  }
}
