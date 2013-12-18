package gdelt

import com.mongodb.casbah.Imports._
import java.io.File

import Utilities._

object gdeltimport extends App {
  val cacheFolder = "/home/saleem/www/gdelt/CACHE"
  val gkgfiles = (new File(cacheFolder) listFiles) filter {
    x => x.getName.endsWith("gkg.csv")
  }

  val coll = Utilities.getCollection
  gkgfiles foreach { f =>
    println(f)
    val entries = processFile(f)
    entries foreach { e =>
      val dbentry = entryToMongoDBObject(e)
      coll.insert(dbentry)
    }
  }
}
