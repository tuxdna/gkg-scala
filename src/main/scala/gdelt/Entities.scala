package gdelt

import com.mongodb.casbah.commons.MongoDBObject
import com.mongodb.casbah.commons.MongoDBList
import com.mongodb.BasicDBObject
import com.mongodb.casbah.Imports._

class GkgRecord(dbObj: MongoDBObject) {
  def date = dbObj.as[String]("date")

  def numarts: Int = {
    val x = dbObj.as[String]("numarts")
    if (x.isEmpty()) 0 else x.toInt
  }

  def counts = dbObj.as[String]("counts")

  def themes: List[String] = {
    (dbObj.as[MongoDBList]("themes") toList) map (_.toString)
  }

  def locations: List[String] = {
    (dbObj.as[MongoDBList]("locations") toList) map (_.toString)
  }

  def persons: List[String] = {
    (dbObj.as[MongoDBList]("persons") toList) map (_.toString)
  }

  def organizations: List[String] = {
    (dbObj.as[MongoDBList]("organizations") toList) map (_.toString)
  }

  lazy val toneObj = dbObj.as[BasicDBObject]("tone")

  def tone = toneObj.as[Double]("tone")

  def pos = toneObj.as[Double]("pos")

  def neg = toneObj.as[Double]("neg")

  def polarity = toneObj.as[Double]("polarity")

  def verb = toneObj.as[Double]("verb")

  def pronoun = toneObj.as[Double]("pronoun")

  def cameoevents = {
    (dbObj.as[MongoDBList]("cameoevents") toList) map (_.toString)
  }

  def sources: List[String] = {
    (dbObj.as[MongoDBList]("sources") toList) map (_.toString)
  }

  def sourceurls: List[String] = {
    (dbObj.as[MongoDBList]("sourceurls") toList) map (_.toString)
  }
}

case class Geography() {
  // GEO_TYPE
  // GEO_FULLNAME
  // GEO_COUNTRYCODE
  // GEO_ADM1CODE
  // GEO_LAT
  // GEO_LONG
  // GEO_FEATUREID
}
