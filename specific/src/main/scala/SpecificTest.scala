
import edu.rit.csh.wikiData.{Rating, Income}
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.{SparkContext, SparkConf}
import Utils._

object SpecificTest {

  def testRatingDir(implicit sc: SparkContext, inputDir: String): Unit = {
    println("loading dir: " + inputDir)
    val avroRDD = sc.hadoopFile[AvroWrapper[Rating], NullWritable, AvroInputFormat[Rating]](inputDir)
    println("starting execution")
    avroRDD.map(elem => (elem._1.datum.getMovieId, elem._1.datum))
           .groupByKey()
           .map { case (movie, ratings) => (movie, ratings.map(_.getRating).filter(notNull).avg()) }
           .take(20)
           .foreach(println)
  }

  def testIncomeDir(sc: SparkContext, inputDir: String): Unit = {
    println("loading dir: " + inputDir)
    val avroRDD = sc.hadoopFile[AvroWrapper[Income], NullWritable, AvroInputFormat[Income]](inputDir)
    println("starting execution")
    avroRDD.map(elem => (elem._1.datum.getGeography, elem._1.datum))
           .groupByKey()
           .map { case (geo, income) => (geo, income.map(_.getMedianIncome).avg) }
           .take(20)
           .foreach(println)
  }

  def main(args: scala.Array[String]) = {
    val conf = new SparkConf()
          .setAppName("Specific Current Test")
          .registerKryoClasses(Array(classOf[Income], classOf[Rating]))
          .set("spark.eventLog.enabled", "true")

    implicit val sc = new SparkContext(conf)

    val dirs = List("/home/jd/Documents/spark/ratings_1m",
                    "/home/jd/Documents/spark/ratings_10m",
                    "/home/jd/Documents/spark/ratings")

    testIncomeDir(sc, "/home/jd/Documents/spark/income")
    dirs.foreach(implicit dir => testRatingDir)
  }
}
