package gdelt

import com.mongodb.casbah.Imports._
import scala.collection.mutable
import graph.EntityGraph

object gdeltgrah2 extends App {
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
  var nodeId = 0

  val personGraph = new EntityGraph("/tmp/personsGraph-neo4j")
  personGraph.withinTransaction {
    for ((e, index) <- allEntries.zipWithIndex) {
      val record = new GkgRecord(e)
      print("" + record.date + "  -- " + index + "" + "\r")

      if (record.themes.exists(_.toLowerCase.contains(filterTheme)) &&
        record.locations.exists(_.toLowerCase.contains(filterLocation))) {
        //entry matches the theme and also matches the filter location

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
        personGraph.processRecord(elements)
      }
    }
  }

  personGraph.print
}
