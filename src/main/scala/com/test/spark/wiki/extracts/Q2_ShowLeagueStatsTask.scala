package com.test.spark.wiki.extracts

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._

case class Q2_ShowLeagueStatsTask(bucket: String) extends Runnable {
  private val session: SparkSession = SparkSession.builder().getOrCreate()

  import session.implicits._

  override def run(): Unit = {
    val standings = session.read.parquet("/home/soat/wikiparquet").as[LeagueStanding].cache()

    standings
      // ...code...
        .createTempView("wikiLeague")
      session.sql("select * from wikiLeague")
      .show()

    println("Utiliser createTempView sur $standings et, en sql, afficher la moyenne de buts par saison et par championnat")
    session.sql("select avg(goalsFor), league, season from wikiLeague group by season, league")

    println("En Dataset, quelle est l'équipe la plus titrée de France ?")
    session.sql("select team, season, sum(position) from wikiLeague where position = 1 and league = 'Ligue 1' group by team")

    println("En Dataset, quelle est la moyenne de points des vainqueurs sur les 5 différents championnats ?")


    println("En Dataset, quelle est la moyenne de points d'écart entre le 1er et le 10ème de chaque championnats " +
      "par décennie")

  }
}
