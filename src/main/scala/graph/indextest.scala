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

object indextest extends App {

  def deleteFileOrDirectory(file: File): Unit = {
    if (!file.exists()) return ;
    if (file.isDirectory()) for (child <- file.listFiles()) deleteFileOrDirectory(child)
    else file.delete()
  }

  val GRAPHLOCATION = "/tmp/testgraph"

  // setup Graph DB
  deleteFileOrDirectory(new File(GRAPHLOCATION))
  val graphDb = new GraphDatabaseFactory().newEmbeddedDatabaseBuilder(GRAPHLOCATION)
    .setConfig(GraphDatabaseSettings.node_keys_indexable, "name")
    .setConfig(GraphDatabaseSettings.node_auto_indexing, "true")
    .newGraphDatabase()

  // register shutdown hook thread
  ShutdownHookThread {
    graphDb.shutdown()
  }

  val tx = graphDb.beginTx
  val indexManager = graphDb.index()
  val nameIdsIndex = // indexManager.forNodes("auto_index", Map("type" -> "exact"))
  graphDb.index().getNodeAutoIndexer().getAutoIndex()

  val node1 = createOrFetchNode("A")
  val node2 = createOrFetchNode("A")

  tx.success()
  tx.finish()

  def createOrFetchNode(nodeName: String) = {
    val hits = nameIdsIndex.get("name", nodeName)
    val node = hits.getSingle()
    println(s"search for $nodeName -> list: ${hits.iterator.toList} node: $node")

    if (node == null) {
      val node2 = graphDb.createNode()
      node2.setProperty("name", nodeName)
      node2
    } else node
  }

}
