import java.sql.DriverManager

import com.typesafe.config
import com.typesafe.config.ConfigFactory

case class Dept(deptno:Int,dname:String,loc:String){
  override def toString: String = deptno +" "+dname+" "+loc
}
object test {
def main(args:Array[String]): Unit ={
  val conf = ConfigFactory.load()
  val props = conf.getConfig("dev")
  val username = props.getString("username")
  val pwd = props.getString("password")
  val host = props.getString("host")
  val db = props.getString("db")
  val driver = "com.mysql.jdbc.Driver"
  val url = "jdbc:mysql://"+host+"/"+db
  println(url)
  Class.forName(driver)
  val connection = DriverManager.getConnection(url,username,pwd)
  val stmt = connection.createStatement()
  val resultset = stmt.executeQuery("select * from dept")
  Iterator.continually(resultset.next,resultset).takeWhile(_._1).map( row =>
      Dept( row._2.getInt("deptno"),row._2.getString("dname"),
    row._2.getString("loc"))).toList.foreach(println)
}

}
