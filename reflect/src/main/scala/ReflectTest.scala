import edu.rit.csh.wikiData.{Rating, Income}
import org.apache.avro.mapred.{AvroJob, AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable
import org.apache.hadoop.mapred.{JobConf, FileInputFormat}
import org.apache.spark.{SparkConf, SparkContext}
import Utils._
object ReflectTest {

   def testRatingDir(implicit sc: SparkContext, inputDir: String): Unit = {
     println("loading dir: " + inputDir)
     val jobConf = new JobConf()
     FileInputFormat.setInputPaths(jobConf, inputDir)
     AvroJob.setInputReflect(jobConf)
     val avroRDD = sc.hadoopRDD(jobConf,
                                classOf[AvroInputFormat[Rating]],
                                classOf[AvroWrapper[Rating]],
                                classOf[NullWritable])
     println("starting execution")
     avroRDD.map(elem => (elem._1.datum.movieId, elem._1.datum))
            .groupByKey()
            .map({ case (movie, ratings) => (movie, ratings.map(_.rating).filter(notNull).avg()) })
            .take(20)
            .foreach(println)
   }

   def testIncomeDir(sc: SparkContext, inputDir: String): Unit = {
     println("loading dir: " + inputDir)
     val jobConf = new JobConf()
     FileInputFormat.setInputPaths(jobConf, inputDir)
     AvroJob.setInputReflect(jobConf)
     val avroRDD = sc.hadoopRDD(jobConf,
                                classOf[AvroInputFormat[Income]],
                                classOf[AvroWrapper[Income]],
                                classOf[NullWritable])
     println("starting execution")
     avroRDD.map(elem => (elem._1.datum.geography, elem._1.datum))
            .groupByKey()
            .map { case (movie, ratings) => (movie, ratings.map(_.median_income).filter(notNull).avg()) }
            .take(20)
            .foreach(println)
   }

   def main(args: Array[String]) = {
     val conf = new SparkConf().setAppName("Reflect Current Test")
     conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
         .registerKryoClasses(Array(classOf[Rating], classOf[Income]))
         .set("spark.eventLog.enabled", "true")

     implicit val sc = new SparkContext(conf)

     val dirs = List("/home/jd/Documents/spark/ratings_1m",
                     "/home/jd/Documents/spark/ratings_10m",
                     "/home/jd/Documents/spark/ratings")

     testIncomeDir(sc, "/home/jd/Documents/spark/income")
     dirs.foreach(implicit dir => testRatingDir)

   }
 }
