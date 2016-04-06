// Casbah tutorial
package tutorial


import com.mongodb.casbah.Imports.$set
import com.mongodb.casbah.Imports.MongoClient
import com.mongodb.casbah.Imports.MongoDBObject

object tutorial extends App {

  import com.mongodb.casbah.Imports._
  val mongoClient = MongoClient("localhost", 27017)
  val db = mongoClient("test")
  println(db.collectionNames)
  val coll = db("test")
  // Optionally drop the collection
  // coll.drop()

  val a = MongoDBObject("hello" -> "world")
  val b = MongoDBObject("language" -> "scala")
  coll.insert( a )
  coll.insert( b )
  println("count() on collection" + coll.count())
  val allDocs = coll.find()
  println( allDocs )
  for(doc <- allDocs) println( doc )

  val hello = MongoDBObject("hello" -> "world")
  val helloWorld = coll.findOne( hello )

  // Find a document that doesn't exist
  val goodbye = MongoDBObject("goodbye" -> "world")
  val goodbyeWorld = coll.findOne( goodbye )


  // update documents
  val query = MongoDBObject("language" -> "scala")
  val update = MongoDBObject("platform" -> "JVM")
  val result = coll.update( query, update )

  println("Number updated: " + result.getN)
  for (c <- coll.find) println(c)

  // update operators
  {
    val query = MongoDBObject("platform" -> "JVM")
    val update = $set("language" -> "Scala")
    val result = coll.update( query, update )
  }

  println( "Number updated: " + result.getN )
  for ( c <- coll.find ) println( c )


  // update, or fallback to insert: upsert
  {
    val query = MongoDBObject("language" -> "clojure")
    val update = $set("platform" -> "JVM")
    val result = coll.update( query, update, upsert=true )
  }
  println( "Number updated: " + result.getN )
  for (c <- coll.find) println(c)

  // removing documents
  {
    val query = MongoDBObject("language" -> "clojure")
    val result = coll.remove( query )
    println("Number removed: " + result.getN)
    for (c <- coll.find) println(c)
  }

  // remove all documents
  {
    val query = MongoDBObject()
    val result = coll.remove( query )

    println( "Number removed: " + result.getN )
    println( coll.count() )
  }
  // remove the whole collection
  coll.drop()
}
