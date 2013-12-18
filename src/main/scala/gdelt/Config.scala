package gdelt

import com.mongodb.casbah.MongoClient

object Config {
  val dbName = "gdelt"
  val collectionName = "gdelt01"
  val dbHost = "localhost"
  val dbPort = 27017
}
