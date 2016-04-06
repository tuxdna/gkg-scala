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

object mygraph2 extends App {

  def deleteFileOrDirectory(file: File): Unit = {
    if (!file.exists()) return ;
    if (file.isDirectory()) for (child <- file.listFiles()) deleteFileOrDirectory(child)
    else file.delete()
  }

  val COOCCUR_RT = new RelationshipType {
    def name(): String = {
      "COOCCUR"
    }
  }

  def createOrFetchNode(nodeName: String, graphDb: GraphDatabaseService) = {
    val indexManager = graphDb.index()
    val autoIndex = graphDb.index().getNodeAutoIndexer().getAutoIndex()

    val hits = autoIndex.get("name", nodeName)
    val node = hits.getSingle()

    if (node != null) {
      // if we already have a node Id
      // fetch it by Id
      val nodeId = node.getId()
      graphDb.getNodeById(nodeId)
    } else {
      // else we create a new node and 
      // also update the node id mapping
      val node = graphDb.createNode()
      node.setProperty("name", nodeName)
      node
    }
  }

  def processRecord(r: Seq[String]) = {
    val elements = r.sorted
    // calculate local co-occurence
    val partialCooccurence = (for (
      i <- (0 until elements.length);
      j <- ((i + 1) until elements.length)
    ) yield (elements(i), elements(j)) -> 1) toMap

    partialCooccurence foreach { elem =>
      val nodes = elem._1
      val value = elem._2

      val (sourceNodeName, targetNodeName) = elem._1
      // update the graph here
      val sourceNode = createOrFetchNode(sourceNodeName, graphDb)
      val targetNode = createOrFetchNode(targetNodeName, graphDb)

      val relCoOccur = {
        // only those cooccurence relationships which connect source and target nodes
        val relationships = sourceNode.getRelationships(COOCCUR_RT).filter {
          r => r.getEndNode().getId() == targetNode.getId()
        }.take(1).toList

        if (relationships.size > 0) relationships(0)
        else {
          sourceNode.createRelationshipTo(targetNode, COOCCUR_RT)
        }
      }

      val count: Int = try {
        val c = if (relCoOccur.hasProperty("COUNT")) relCoOccur.getProperty("COUNT") else 0
        c.asInstanceOf[Int]
      } catch { case x: Throwable => 0 }

      val newCount = count + value
      relCoOccur.setProperty("COUNT", newCount)

      println(s"$sourceNodeName $targetNodeName $newCount")
    }
  }
  def neo4jStoreDir = "/tmp/gdelt-gkg-people"

  // setup Graph DB
  deleteFileOrDirectory(new File(neo4jStoreDir))
  val graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(neo4jStoreDir)
    .setConfig(GraphDatabaseSettings.node_keys_indexable, "name")
    .setConfig(GraphDatabaseSettings.node_auto_indexing, "true")
    .newGraphDatabase()

  // register shutdown hook thread
  ShutdownHookThread {
    graphDb.shutdown()
  }

  // lets begin

  val records = List(List("A", "B", "C"), List("A", "C"), List("B", "C"), List("C", "D", "E"), List("F", "G", "H"), List("F", "X", "H"))

  {
    val tx = graphDb.beginTx

    for (r <- records) {
      val elements = r.sorted
      processRecord(elements)
    }
    tx.success()
    tx.finish()
  }

  // run traversal
  {
    val tx = graphDb.beginTx

    def getFriends(person: Node): Traverser = {
      val td = Traversal.description().breadthFirst()
        .relationships(COOCCUR_RT, Direction.BOTH)
        .evaluator(Evaluators.excludeStartPosition())
      td.traverse(person)
    }

    for (n <- graphDb.getAllNodes() if n.hasProperty("name")) {
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
    tx.success()
    tx.finish()
  }
}
