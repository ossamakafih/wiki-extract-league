package com.test.spark.wiki.extracts

import java.io.{File, FileInputStream, FileReader}
import java.net.URL

import com.fasterxml.jackson.annotation.JsonProperty
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql.EsSparkSQL
import org.joda.time.DateTime
import org.jsoup.Jsoup
import org.jsoup.nodes._
import org.slf4j.{Logger, LoggerFactory}

import scala.collection.mutable.ListBuffer

case class Q1_WikiDocumentsToParquetTask(bucket: String) extends Runnable {
  private val conf = new SparkConf().setAppName("League").setMaster("local[*]")
  private val session: SparkSession = SparkSession.builder().config(conf).getOrCreate()
  private val logger: Logger = LoggerFactory.getLogger(getClass)
  private val sc: SparkContext = session.sparkContext

  override def run(): Unit = {
    val toDate = DateTime.now().withYear(2017)
    val fromDate = toDate.minusYears(40)
    val leagueStanding = new ListBuffer[LeagueStanding]()

    val cfg = Map(
      ("es.resource", "wiki/doc"),
      ("es.net.http.auth.user", "elastic"),
      ("es.net.http.auth.pass", "changeme")
    )

    import session.implicits._

    //val allLeague = sc.parallelize(getLeagues)


    val allLeagueStanding = getLeagues
        .toDS()
      .flatMap {
      input =>
        (fromDate.getYear until toDate.getYear).map {
          year =>
            year + 1 -> (input.name, input.url.format(year, year + 1))
        }
    }.flatMap{

          case(season, (league, url)) => {
            try {
              val doc: Document = Jsoup.connect(url).get()
              val parseHtml = Jsoup.parse(doc.html())

              val table = parseHtml.getElementsByClass("wikitable gauche").select("tbody")
              val rows = table.select("tr")
              for(i <- 1 to rows.size()) {

                val row = rows.get(i)
                val rank = row.select("span").get(0).text().toInt
                val team = row.select("span").get(1).text().toLowerCase
                val point = row.select("td").get(2).text().toInt
                val played = row.select("td").get(3).text().toInt
                val won = row.select("td").get(4).text().toInt
                val drawn = row.select("td").get(5).text().toInt
                val lost = row.select("td").get(6).text().toInt
                val goalsFor = row.select("td").get(7).text().toInt
                val goalsAgainst = row.select("td").get(8).text().toInt
                val goalsDifference = row.select("td").get(9).text().toInt
                //println((league, season, rank, team, point, played, won, drawn, lost, goalsFor, goalsAgainst, goalsDifference))
                leagueStanding += LeagueStanding(league, season, rank, team, point, played, won, drawn, lost, goalsFor, goalsAgainst, goalsDifference)

              }

              leagueStanding
              }

             catch {
              case _:Throwable => {
                logger.warn(s"Can't parse season $season from $url")
                Seq.empty
              }

            }
        }}.toDF()

    EsSparkSQL.saveToEs(allLeagueStanding, cfg = cfg)
    
      allLeagueStanding
        .coalesce(numPartitions=2)
      .write
      .mode(SaveMode.Overwrite)
        .parquet(bucket)

  }

  private def getLeagues: Seq[LeagueInput] = {
    val mapper = new ObjectMapper(new YAMLFactory())
    mapper.registerModule(DefaultScalaModule)
    val filename = "src/main/ressources/leagues.yaml"
    val inputStream = new FileInputStream(new File(filename))
    mapper.readValue(inputStream, classOf[Array[LeagueInput]]).toSeq

  }

}

case class LeagueInput(name: String,
                       url: String)

case class LeagueStanding(league: String,
                          season: Int,
                          position: Int,
                          team: String,
                          points: Int,
                          played: Int,
                          won: Int,
                          drawn: Int,
                          lost: Int,
                          goalsFor: Int,
                          goalsAgainst: Int,
                          goalsDifference: Int)
