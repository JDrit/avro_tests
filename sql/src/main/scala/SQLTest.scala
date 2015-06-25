
import org.apache.avro.generic.GenericRecord
import org.apache.avro.mapred.{AvroInputFormat, AvroWrapper}
import org.apache.hadoop.io.NullWritable
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import com.databricks.spark.avro._

object SQLTest {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("SQL Test")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.eventLog.enabled", "true")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)

    val dirs = List("/home/jd/Documents/spark/ratings_1m",
      "/home/jd/Documents/spark/ratings_10m",
      "/home/jd/Documents/spark/ratings")
    //val test = (rating: Double) => rating > 5
    //sqlContext.udf.register("test", test)
    //avro.select("rating").collect()
    //avro.registerTempTable("avro")
    //sqlContext.sql("select * from avro where test(rating)").collect()

    //val incomeDf = sqlContext.avroFile("/home/jd/Documents/spark/income")
    //incomeDf.groupBy("geography").avg("median_income").take(20).foreach(println)

    //dirs.foreach { dir =>
    //  sqlContext.avroFile(dir).groupBy("movieId").avg("rating").take(20).foreach(println)
    //}
    val avroDf = sqlContext.avroFile(dirs.last)
    avroDf.registerTempTable("avro")
    sqlContext.sql("select movieId, GROUPING__ID from avro").take(20).foreach(println)

    var startTime = System.currentTimeMillis()
    sqlContext.avroFile(dirs.last).groupBy("movieId").sum("rating").select("movieId", "SUM(rating)").take(20).foreach(println)
    println("SQL time: " + (System.currentTimeMillis() - startTime))
/*
    startTime = System.currentTimeMillis()
    val avroDf = sqlContext.avroFile(dirs.last)
    avroDf.registerTempTable("avro")
    sqlContext.sql("select movieId, SUM(rating) from avro group by movieId limit 20").collect()
    println("SQL time: " + (System.currentTimeMillis() - startTime))

    startTime = System.currentTimeMillis()
    val avroRDD = sc.hadoopFile[AvroWrapper[GenericRecord], NullWritable, AvroInputFormat[GenericRecord]](dirs.last)
    avroRDD.map(elem => (elem._1.datum.get("movieId").asInstanceOf[Long], elem._1.datum.get("rating").asInstanceOf[Double]))
           .groupByKey()
           .map(t => (t._1, t._2.sum))
           .take(20)
           .foreach(println)
    println("RDD time: " + (System.currentTimeMillis() - startTime))

    startTime = System.currentTimeMillis()
    sqlContext.sql("select movieId, SUM(rating) as s from avro group by movieId order by s desc limit 20").collect()
    println("SQL complex time: " + (System.currentTimeMillis() - startTime))

    startTime = System.currentTimeMillis()
    avroDf.select("movieId", "rating").groupBy("movieId").sum("rating").orderBy("SUM(rating)").limit(20).collect()
    println("SQL complex 2 time: " + (System.currentTimeMillis() - startTime))

    startTime = System.currentTimeMillis()
    avroRDD.map(elem => (elem._1.datum.get("movieId").asInstanceOf[Long], elem._1.datum.get("rating").asInstanceOf[Double]))
           .groupByKey()
           .map(t => (t._2.sum, t._1))
           .sortByKey(false)
           .take(20)
    println("RDD complex time: " + (System.currentTimeMillis() - startTime))*/
  }

}
