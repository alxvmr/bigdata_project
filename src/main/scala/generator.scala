object generator {

  case class Event(NOC: String, Year: String, City: String, Medal: String) {
    //override def toString: String = s"${NOC} ${Year} ${City} ${Medal}"
    override def toString: String = NOC + " " + Year + " " + City + " " + Medal;
  }

  val NOC: List[String] = List("RUS", "US", "GER", "FRC", "CHN");
  val Year: List[Int] = List(2000, 2002, 2004, 2006, 2008, 2010, 2012, 2014, 2016, 2018, 2020);
  val City: List[String] = List("New_York", "Moscow", "Berlin", "Paris", "Pekin");
  val Medal: List[String] = List("Gold", "Silver", "Bronze");

  val start = 0
  val end_y = Year.size
  val end_c = City.size
  val end_m = Medal.size

  def generate(): Event = {
    val rnd = new scala.util.Random

    val r_noc = rnd.nextInt(NOC.size - start)
    val n = NOC(r_noc).toString()
    val r_year = rnd.nextInt(end_y - start)
    val y = Year(r_year).toString()
    val r_city = rnd.nextInt(end_c - start)
    val c = City(r_city).toString()
    val r_medal = rnd.nextInt(end_m - start)
    val m = Medal(r_medal).toString
    Event(n, y, c, m)
  }
}