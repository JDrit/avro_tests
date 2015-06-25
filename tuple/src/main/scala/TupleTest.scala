
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkContext, SparkConf}
import Utils._
import wikiData.{Income, Rating}

object TupleTest {

  def testRatingDir(implicit sc: SparkContext, inputDir: String): Unit = {
    println("loading dir: " + inputDir)
    val avroRDD = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](inputDir)
    println("starting execution")
    avroRDD.map({ elem =>
      val record = elem._1.datum
      (record.get("movieId").asInstanceOf[Long], (record.get("movieId").asInstanceOf[Long], record.get("userId").asInstanceOf[Long], record.get("rating").asInstanceOf[Double], record.get("timestamp").asInstanceOf[Long]))})
      .groupByKey()
      .map({ case (movie, ratings) => (movie, ratings.map(_._4).filter(notNull).avg) })
      .take(20)
      .foreach(println)
  }

  def testIncomeDir(sc: SparkContext, inputDir: String): Unit = {
    println("loading dir: " + inputDir)
    val avroRDD = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](inputDir)
    println("starting execution")
    avroRDD.map({ elem =>
      val record = elem._1.datum
      (record.get("geography").toString, (record.get("id1").toString, record.get("id2").toString, record.get("geography").toString, record.get("median_income").asInstanceOf[Long]))})
      .groupByKey()
      .map({ case (geo, income) => (geo, income.map(_._4).avg) })
      .take(20)
      .foreach({case (geo, income) => println(geo + " - " + income)})
  }

  def main(args: scala.Array[String]) = {
    val conf = new SparkConf()
      .setAppName("Tuple Current Test")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.eventLog.enabled", "true")
    implicit val sc = new SparkContext(conf)

    val dirs = List("/home/jd/Documents/spark/ratings_1m",
                    "/home/jd/Documents/spark/ratings_10m",
                    "/home/jd/Documents/spark/ratings")

    testIncomeDir(sc, "/home/jd/Documents/spark/income")
    dirs.foreach(implicit dir => testRatingDir)

  }
}