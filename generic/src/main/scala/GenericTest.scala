import org.apache.avro.Schema.Parser
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.avro.{Schema, SchemaNormalization}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkConf, SparkContext}
import Utils._

object GenericTest {

  val incomeSchema =
    """{
        "type" : "record",
        "name" : "Income",
        "namespace" : "edu.rit.csh.wikiData",
        "doc" : "Schema generated by Kite",
        "fields" : [ {
          "name" : "id1",
          "type" : [ "null", "string" ],
          "doc" : "Type inferred from '8600000US00601'",
          "default" : null
        }, {
          "name" : "id2",
          "type" : [ "null", "string" ],
          "doc" : "Type inferred from '00601'",
          "default" : null
        }, {
          "name" : "geography",
          "type" : [ "null", "string" ],
          "doc" : "Type inferred from '00601 5-Digit ZCTA, 006 3-Digit ZCTA'",
          "default" : null
        }, {
          "name" : "median_income",
          "type" : [ "null", "long" ],
          "doc" : "Type inferred from '11102'",
          "default" : null
        } ]
      }
    """

  val ratingSchema =
    """{
        "type" : "record",
        "name" : "Rating",
        "namespace" : "edu.rit.csh.wikiData",
        "doc" : "Schema generated by Kite",
        "fields" : [ {
          "name" : "userId",
          "type" : [ "null", "long" ],
          "doc" : "Type inferred from '1'",
          "default" : null
        }, {
          "name" : "movieId",
          "type" : [ "null", "long" ],
          "doc" : "Type inferred from '253'",
          "default" : null
        }, {
          "name" : "rating",
          "type" : [ "null", "double" ],
          "doc" : "Type inferred from '3.0'",
          "default" : null
        }, {
          "name" : "timestamp",
          "type" : [ "null", "long" ],
          "doc" : "Type inferred from '900660748'",
          "default" : null
        } ]
      }"""

  lazy val incomeFingerprint = SchemaNormalization.parsingFingerprint64(new Schema.Parser().parse(incomeSchema))
  lazy val ratingFingerprint = SchemaNormalization.parsingFingerprint64(new Schema.Parser().parse(ratingSchema))

  def testRatingDir(implicit sc: SparkContext, inputDir: String): Unit = {
    println("loading dir: " + inputDir)
    val avroRDD = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](inputDir)
    println("starting execution")
    avroRDD.map(elem => (elem._1.datum.get("movieId").asInstanceOf[Long], elem._1.datum))
      .groupByKey()
      .map({ case (movie, ratings) => (movie, ratings.map(_.get("rating").asInstanceOf[Double]).filter(notNull).avg()) })
      .take(20)
      .foreach(println)
  }

  def testIncomeDir(sc: SparkContext, inputDir: String): Unit = {
    println("loading dir: " + inputDir)
    val avroRDD = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](inputDir)
    println("starting execution")
    avroRDD.map(elem => (elem._1.datum.get("geography").toString, elem._1.datum))
      .groupByKey()
      .map({ case (geo, income) => (geo, income.map(_.get("median_income").asInstanceOf[Long]).avg) })
      .take(20)
      .foreach(println)
  }

  def main(args: Array[String]) = {
    val parser = new Parser()

    val conf = new SparkConf()
      .setAppName("Generic Current Test")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.eventLog.enabled", "true")
      //.registerAvroSchema(Array(parser.parse(incomeSchema), parser.parse(ratingSchema)))


    implicit val sc = new SparkContext(conf)

    val dirs = List("/home/jd/Documents/spark/ratings_1m",
      "/home/jd/Documents/spark/ratings_10m",
      "/home/jd/Documents/spark/ratings")

    testIncomeDir(sc, "/home/jd/Documents/spark/income")
    dirs.foreach(implicit dir => testRatingDir)
  }
}