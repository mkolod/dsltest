package com.alpine.dsl.external

import scala.util.parsing.combinator._
import java.sql.DriverManager
import org.apache.spark.SparkContext
import scala.reflect.api.TypeTags
import scala.reflect.runtime.{universe => ru}
import javax.management.Query
import scala.concurrent.{ future, promise }
import scala.concurrent.ExecutionContext.Implicits.global


/**
 * Created by marek on 4/18/14.
 */

sealed trait DataSource

sealed trait Queryable {
  def execute(query: String): String
}

case class MySQL(uri: String, user: String, password: String) extends DataSource with Queryable {

  Class.forName("com.mysql.jdbc.Driver")

  def execute(query: String): String = {

    val conn = DriverManager.getConnection(uri, user, password)
    val stmt = conn.createStatement()
    val rs = stmt.executeQuery(query)
    val sb = new StringBuilder()
    while (rs.next) {
      sb.append(rs.getString("name") + "\n")
    }
    sb.toString()
  }
}

sealed trait FlatFile
case class HDFS() extends FlatFile
case class LocalFS() extends FlatFile


case class Person(name: String, age: Int)

case class SparkSQL(master: String = "local[4]", appName: String = "defaults",
                 sparkHome: String = null, jars: Seq[String] = Nil) extends DataSource with Queryable {
  val sc = new SparkContext(master = master, appName = "dslDemo")

  def execute(query: String): String = {
    def getTypeTag[T: ru.TypeTag](obj: T) = ru.typeTag[T]
    implicit val tt = getTypeTag(Person("foo", 1))
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext._
    val people = sc.textFile("/Users/marek/src/spark/examples/src/main/resources/people.txt")
      .map(_.split(",")).map(p => Person(p(0), p(1).trim.toInt))
    people.registerAsTable("people")
    sql(query).collect().mkString(",")

   // val teenagers = sql("SELECT name FROM people WHERE age >= 13 AND age <= 19")
   // teenagers.map(t => "Name: " + t(0)).collect().mkString(",")
  }

}

case class Web(uri: String) extends DataSource

sealed trait AbstractQuery
case class QueryLiteral(query: String) extends AbstractQuery
case class QueryBuilder(features: List[String], table: String, limit: Option[Int] = None) extends AbstractQuery {

  val projection = s"SELECT ${features.mkString(",")} FROM $table"
  def query = limit match {
    case Some(num) =>  s"$projection LIMIT ${limit.get}"
    case None => projection
  }
}

class ExternalDSL extends JavaTokenParsers {

  private def rmQ(s: String) =
    if (s.startsWith("\"") && s.endsWith("\"")) s.substring(1, s.length - 1) else s

  def userAuth = "as" ~> "user" ~> userName
  def passwordAuth = "with" ~> "password" ~> password

  def src = "get" ~> "data" ~> "from" ~> dataSource
  def atUri = "at" ~> uri

  def source = src ~ atUri ~ opt(userAuth) ~ opt(passwordAuth) ^^ {
    case "SparkSQL" ~ uri ~ None ~ None => SparkSQL(master = rmQ(uri))
    case "MySQL" ~ uri ~ Some(user) ~ Some(password) => MySQL(rmQ(uri), rmQ(user), rmQ(password))
  }

  def dataSource = literal("SparkSQL") | literal("MySQL") | literal("Postgres")

  def query = (usingQuery | queryDSL) ^^ {
    case q: QueryLiteral => q
    case q: QueryBuilder => s"SELECT ${q.features.mkString(",")} FROM ${q.table} ${if (q.limit != None) s"LIMIT ${q.limit.get}"}"
  }
  def usingQuery = "using" ~> "query" ~> queryStr ^^ { QueryLiteral(_) }

  def queryDSL = usingFeatures ~ fromTable ~ opt(limit) ^^ {
    case feats ~ tbl ~ lim =>
      lim match {
        case Some(value) => QueryBuilder(feats.toList.map(rmQ), rmQ(tbl.toString), Some(lim.get.toInt))
        case None => QueryBuilder(feats.toList.map(rmQ), rmQ(tbl.toString))
      }
  }

  def usingFeatures = "using" ~> "features" ~> features
  def fromTable = "from table" ~> table
  def features = repsep(feature, ",")
  def feature = stringLiteral
  def limit = "limit" ~> limitCount
  def limitCount = wholeNumber
  def table = stringLiteral

  def uri = stringLiteral
  def userName = stringLiteral
  def password = stringLiteral
  def queryStr = stringLiteral

}

object Main extends App {

  val testStr1 =
    """get data from MySQL at "jdbc:mysql://localhost:3306/" as user "root" with password ""
       using query "SELECT * FROM sonar.events"
       """.stripMargin

  val testStr2 =
    """get data from MySQL at "jdbc:mysql://localhost:3306/" as user "root" with password ""
       using features "name","resource_id","category" from table "sonar.events" limit 10
    """.stripMargin

  val testStr3 =
    """get data from SparkSQL at "local[4]" using features "foo","bar","baz"
    """.stripMargin

  val testStr0 = """get data from SparkSQL at "local[4]" using features "name","age" from table "people" limit 10"""
  val p = new ExternalDSL

  val srcResult = p.parse(p.source, testStr0)
  val p.Success(backend, qryText) = srcResult
  val p.Success(qry, _) = p.parse(p.query, qryText)

  val f = future {
    backend.asInstanceOf[Queryable].execute(qry.toString)
  }

  f onSuccess { case s =>  println(s) }
  f onFailure { case t => t.printStackTrace() }

}