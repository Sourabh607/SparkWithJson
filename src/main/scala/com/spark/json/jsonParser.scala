package com.spark.json

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, explode_outer, from_json}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, SaveMode, SparkSession}

object jsonParser {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    System.setProperty("hadoop.home.dir", "E:\\experimental_stuff\\Hadoop")
    val spark = SparkSession.builder().appName("JsonParser").master("local[3]").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    import spark.implicits._

    val jsonStr = """{"Zipcode":704,"ZipCodeType":"STANDARD","City":"PARC PARQUE","State":"PR"}"""
    val nestedJsonData = Seq(
      Row("James",List("Java","Scala","C++"),Map("hair"->"black","eye"->"brown")),
      Row("Michael",List("Spark","Java","C++",null),Map("hair"->"brown","eye"->null)),
      Row("Robert",List("CSharp","Python",""),Map("hair"->"red","eye"->"")),
      Row("Washington",null,null),
      Row("Jeferson",List(),Map())
    )
    //Reading json file
    val df1 = spark.read.json("E:\\PysparkWorshop\\SparkWithJson\\src\\main\\resources\\zipcodes.json")

    //Reading multiline Json file
    val df2 = spark.read.option("multiline", "true")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "corrupt_record")
      .json("E:\\PysparkWorshop\\SparkWithJson\\src\\main\\resources\\multiline-zipcode.json")

    //Reading multiple multiline json files from a directory
    val df3 = spark.read.option("multiline", "true")
      .option("mode", "PERMISSIVE")
      .option("columnNameOfCorruptRecord", "corrupt_record")
      .json("E:\\PysparkWorshop\\SparkWithJson\\src\\main\\resources\\zipcodes_streaming")

    //Reading text file containing json string
    val df4 = spark.read.text("E:\\PysparkWorshop\\SparkWithJson\\src\\main\\resources\\samp.txt")

    //Reading json string in a dataframe to parse json
    val df5 = spark.read.json(Seq(jsonStr).toDS())

    // Defining Rdd with Json String
    val rdd6 = spark.sparkContext.parallelize(jsonStr :: Nil)

    //Reading Json from an RDD[String]
    val df6 = spark.read.json(rdd6)


    println("Printing df1 Schema and data : ")
    df1.printSchema()
    df1.show()
    println("Printing df2 Schema and data : ")
    df2.printSchema()
    df2.show()
    println("Printing df3 Schema and data : ")
    df3.printSchema()
    df3.show()
    println("Printing df4 Schema and data : ")
    df4.printSchema()
    df4.show(false)
    println("Printing df5 Schema and data : ")
    df5.printSchema()
    df5.show()
    println("Printing df6 Schema and data : ")
    df6.printSchema()
    df6.show()


    df2.createOrReplaceTempView("df2")

    val read = spark.sqlContext.sql(s""" Select * from df2 where RecordNumber = 10 """)
    read.show()

    //defining schema for df4--
    val schema4 = new StructType()
      .add("Zipcode", LongType, true)
      .add("ZipCodeType", StringType, true)
      .add("City", StringType, true)
      .add("State", StringType, true)

    //defining schema for df7--
    val schema7 = new StructType()
      .add("Name", StringType, true)
      .add("knownLanguages", ArrayType(StringType), true)
      .add("properties", MapType(StringType, StringType), true)


    //Parsing json  from json string in text file
    val jsonParsed = df4.select(from_json(col("value"), schema4).as("json"))
      .select("json.*")

    jsonParsed.printSchema()
    jsonParsed.show(false)

    val df7_1 = spark.createDataFrame(spark.sparkContext.parallelize(nestedJsonData), schema7)
    println("Printing df7_1 Schema and data : ")
    df7_1.printSchema()
    df7_1.show(false)

    val df7_2 = df7_1.select($"Name", explode_outer($"knownLanguages").as("Languages"),$"properties")
    df7_2.show(false)
    df7_2.select($"Name", $"Languages", explode_outer($"properties")).show(100, false)

    read.write.mode(SaveMode.Overwrite).json("E:\\jsonFile.json") //chk
    //new

  }
}
