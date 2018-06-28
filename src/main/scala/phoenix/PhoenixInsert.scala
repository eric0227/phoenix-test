package phoenix

import java.sql.Timestamp
import java.util.Date

import org.apache.hadoop.conf.Configuration
import org.apache.spark.sql.{SparkSession}
import org.apache.phoenix.spark._

object PhoenixInsert {
  /** 피닉스 테이블 생성 스크립트 **/
  """
    |$ /home/otdev/Apps/phoenix/bin/sqlline.py localhost
    |
    |DROP TABLE IF EXISTS TMP_INDEX_TEST;
    |CREATE TABLE IF NOT EXISTS TMP_INDEX_TEST (
    |  YYYYMMDD       VARCHAR(8) NOT NULL,
    |  ID             INTEGER NOT NULL,
    |  DATETIME       TIMESTAMP
    |  CONSTRAINT pk PRIMARY KEY (YYYYMMDD, ID)
    |);
    |CREATE LOCAL INDEX IF NOT EXISTS TMP_INDEX_TEST_INDEX ON TMP_INDEX_TEST(DATETIME);
    |
    | # PK 조건 쿼리
    | select * from TMP_INDEX_TEST where YYYYMMDD = '20180110' order by YYYYMMDD limit 10;
    |
    | # INDEX 조건 쿼리
    | select * from TMP_INDEX_TEST where DATETIME > TO_TIMESTAMP('2018-05-01 01:01:01') order by DATETIME limit 10;
    |
    | # INDEX 없는 쿼리
    | select * from TMP_INDEX_TEST where ID = 10 order by ID limit 10;

  """.stripMargin

  val zookeeper = "server01:2181"
  val dataCount = 1000000

  case class TMP_INDEX_TEST(YYYYMMDD: String = "20180105", ID: Int = 10, DATETIME: Timestamp = new java.sql.Timestamp(new Date().getTime)) {
    def createRandom(): TMP_INDEX_TEST = {
      val r = scala.util.Random
      val ymd = "20" +"%02d".format(r.nextInt(50)) + "%02d".format(r.nextInt(12)) + "%02d".format(r.nextInt(30))
      val id = r.nextInt(10000000)
      val datetime = new java.sql.Timestamp(new Date().getTime + r.nextInt(1000 * 60 * 60))

      TMP_INDEX_TEST(ymd, id, datetime)
    }
  }

  def watchTime[T](name : String, min : Int = 0)(block : => T) : T = {
    val start = System.nanoTime()
    val ret = block
    val end = System.nanoTime()

    import scala.concurrent.duration._
    import scala.language.postfixOps

    val elapsed = (end - start ) nanos

    if (elapsed.toMillis > min) {
      println(s"code $name takes ${elapsed.toMillis} millis seconds.")
    }
    ret
  }

  def main(args: Array[String]): Unit = {

    val bulder = SparkSession.builder()
      .appName("PhoenixInsert")
      .master("local[4]")
    val spark = bulder.getOrCreate()

    println(s"TMP_INDEX_TEST 데이터 생성 ($dataCount)")
    val data = (1 to dataCount).map(i => TMP_INDEX_TEST().createRandom())
    val rdd = spark.sparkContext.parallelize(data)
    val sourceDF = spark.sqlContext.createDataFrame(rdd)
    sourceDF.printSchema()
    //sourceDF.show(numRows = 10, truncate = false)

    val configuration = new Configuration()
    configuration.set("hbase.zookeeper.quorum", zookeeper)

    watchTime("overwrite to phoenix") {
      sourceDF.write
        .format("org.apache.phoenix.spark")
        .mode("overwrite")
        .option("table", "TMP_INDEX_TEST")
        .option("zkUrl", zookeeper)
        .save()
      println(s"ok.")
    }
    println()

    val df = watchTime("Load Phoenix RDD") {
      spark.sqlContext.phoenixTableAsDataFrame(
        "TMP_INDEX_TEST"
        , Array[String]()
        , conf = configuration
      ).cache()
    }
    println()

    val count = watchTime("RDD count") {
      df.count()
    }
    println(s"Total count: $count")
    println()


    watchTime("YYYYMMDD, ID 기준 sort, select top10") {
      df.sort("YYYYMMDD", "ID")
        .show(numRows = 10, truncate = false)
    }
    println()


    watchTime("DATETIME 기준 sort, select top10") {
      df.sort("DATETIME")
        .show(numRows = 10, truncate = false)
    }
  }
}
