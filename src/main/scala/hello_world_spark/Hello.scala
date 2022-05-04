package hello_world_spark

import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import scala.jdk.CollectionConverters._

object Hello {



  def main(args:Array[String]): Unit ={

    val spark = SparkSession.builder()
      .master("local[2]")
      .appName("HelloWorldSpark")
      .getOrCreate();


    val schema = new StructType().add(new StructField("text", StringType, false))

    val data = Row("Hola Mundo") :: Row("desde Spark") :: Nil

    spark.createDataFrame(data.asJava, schema).show()


  }

}
