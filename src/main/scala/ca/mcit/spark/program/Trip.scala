package ca.mcit.spark.program

case class Trip(start_date: String,start_station_code : Int,end_date :String ,end_station_code: Int,duration_sec: Int,is_member : Int)
object Trip {
  def apply(csv: String): Trip = {
    val fields = csv.split(",",-1)
    Trip(fields(0),fields(1).toInt,fields(2),fields(3).toInt,fields(4).toInt,fields(5).toInt)
  }
}