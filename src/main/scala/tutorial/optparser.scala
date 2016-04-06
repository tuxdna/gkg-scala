package tutorial

// TODO: This is incomplete.
class OptParser(options: Map[String, Symbol]) {

  //  val options = Map(
  //    "--cachePath" -> 'string,
  //    "--port" -> 'integer,
  //    "--host" -> 'string
  //    )

  val validTypes = List('string, 'integer)

  def isOption(s: String) = s.length() > 0 && s(0) == '-'

  def parseArgs(args: Array[String]) = {
    var otherList = List[String]()
    def parse(argsList: List[String]): Map[String, Any] = {
      argsList match {
        case optionName :: b :: tail => {
          if (options.contains(optionName)) {
            Map(optionName -> b) ++ parse(tail)
          } else {
            otherList = otherList ::: List(b)
            parse(b :: tail)
          }
        }
        case List(x) => {
          otherList = otherList ::: List(x)
          Map[String, Any]()
        }
        case x => Map[String, Any]()
      }
    }
    val argsList = args.toList
    val m = parse(argsList)
    (m, otherList)
  }

}