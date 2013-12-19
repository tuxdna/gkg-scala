package gdelt

import scala.io.Source
import java.io.File
import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.MongoClient
import scala.xml.XML
import java.io.PrintWriter

object Utilities {

  def getCollection = {
    val mongoClient = MongoClient(Config.dbHost, Config.dbPort)
    val db = mongoClient(Config.dbName)
    val coll = db(Config.collectionName)
    coll
  }

  def processFile(f: File) = {
    val source = Source.fromFile(f)
    // skip header line
    val entries = source.getLines.drop(1).map { l => l.split("\t") }
    entries
  }

  def entryToMongoDBObject(e: Array[String]) = {
    val Array(date, numarts, countevents, themes, locations, persons,
      organizations, tonepack, cameoevents, sources, sourceurls) = e
    val Array(tone, pos, neg, polarity, verb, pronoun) = (tonepack.split(",")) map { _.toDouble }

    MongoDBObject(
      // primary attibutes
      "date" -> date,
      "numarts" -> numarts,
      "counts" -> countevents,
      "themes" -> themes.split(";").filter(x => !x.isEmpty),
      // geograpy
      "locations" -> locations.split(";").filter(x => !x.isEmpty),
      "persons" -> persons.split(";").filter(x => !x.isEmpty),
      "organizations" -> organizations.split(";"),
      // emotion
      "tone" -> Map("tone" -> tone, "pos" -> pos,
        "neg" -> neg, "ploarity" -> polarity,
        "verb" -> verb, "pronoun" -> pronoun),
      // linked events and source information
      "cameoevents" -> cameoevents.split(",").filter(
        x => !x.isEmpty).map {
          c => try { c.toInt } catch { case _: Throwable => -1 }
        },
      "sources" -> sources.split(";"),
      "sourceurls" -> sourceurls.split("<UDIV>"))
  }

  def toJson(a: Any): String = {
    a match {
      // number
      case m: Number => m.toString
      // string
      case m: String => "\"" + m + "\""
      // map
      case m: Map[_, _] => {
        "{" + (m map { x => val key = x._1; toJson(key) + ": " + toJson(m(key)) } mkString (", ")) + "}"
      }
      // list
      case l: Seq[_] => { "[" + (l map (toJson(_)) mkString (",")) + "]" }
      // for anything else: tuple
      case m: Product => toJson(m.productIterator.toList)
      case m: AnyRef => "\"" + m.toString + "\""
    }
  }

  def dumpXmlToJson(xmlPath: String, jsonPath: String) = {
    val graph = XML.load(xmlPath)

    val vertices = graph \\ "node" map { node =>
      (node.attribute("name").get.text, node.attribute("id").get.text.toInt)
    } sortBy (_._2)

    val edges = graph \\ "edge" map { node =>
      (node.attribute("id").get.text.toInt,
        node.attribute("source").get.text.toInt,
        node.attribute("target").get.text.toInt,
        node.attribute("weight").get.text.toDouble)
    }

    // take only those links whose normalized cooccurence is >= 1 percent
    val filteredEdges = edges
    // val filteredEdges = edges.filter(k => k._4 >= 0.01).sortBy(x => x._4).reverse.take(50)

    val random = new java.util.Random()

    val jsonGraph = Map(
      "nodes" -> (vertices.map(x => Map(
        "name" -> x._1,
        "group" -> random.nextInt(10)))),

      "links" -> (filteredEdges.map { x =>
        Map(
          "source" -> x._2, "target" -> x._3,
          "value" -> ((x._4 * 100) toInt))
      }))

    // println(jsonGraph)
    // println(various.tojson.toJson(jsonGraph))
    val outFile = new File(jsonPath)
    println("Writing output to " + outFile.getAbsolutePath())

    val out = new PrintWriter(outFile)
    out.print(toJson(jsonGraph))
    out.close()
  }

  def deleteFileOrDirectory(file: File): Unit = {
    if (!file.exists()) return ;
    if (file.isDirectory()) for (child <- file.listFiles()) deleteFileOrDirectory(child)
    else file.delete()
  }
}