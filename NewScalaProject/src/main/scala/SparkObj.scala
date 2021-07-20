
//import org.apache.spark
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.{col, expr, lag, regexp_replace, to_date, when}
import org.apache.spark.sql.{DataFrame, SparkSession}
//import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

object SparkObj {

    def main(args: Array[String]): Unit = {
      val spark = SparkSession.builder.master("local").appName("Sparkassignment-Training").getOrCreate

    //  val data = spark.read.options(Map("header" -> "true")).csv("C:/Users/harsh/IdeaProjects/NewScalaProject/data/NewData/sampleSocialMedia/sampleData2.csv")
     // data.show()


      val dfInputA: DataFrame = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .json("data/NewData/sampleData.json")
      dfInputA.show()

      val dfSchema: DataFrame = spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .json("data/NewData/schema.json")
      dfSchema.show()
      dfSchema.printSchema()

      println("schema DF")
      val dfResult: DataFrame = spark.read
        .option("header", "true")
        .schema(dfSchema.schema)
        .json("data/NewData/sampleData.json")
      dfResult.show()

      //second way
      val dfRenamed:DataFrame = dfInputA.selectExpr("col1 as colA", "col2 as colB", "col3 as colC")
      //dfInputA.select(col("col1").as("colA"))
      dfRenamed.show()

      val dfInputSocialMedia = spark.read.format("csv")
        .option("inferSchema", "true")
        .option("header", "true")
        .load("data/NewData/sampleSocialMedia/sampleData2.csv")
      dfInputSocialMedia.show()

      //3rd way
      val ColsSeq: Seq[String] = Seq(
        "event_timestamp"
        ,"visitor_id"
        ,"session_id"
        ,"device_type"
        ,"item_code"
        ,"redirection_source"
      )

      //new cols Sequence
      val renamedColsSeq: Seq[String] = Seq(
        "event_ts"
        ,"visitor_id",
        "session_id",
        "device_type"
        ,"item_code"
        ,"sourced_from"
      )

      val dfInputOldNames: DataFrame = dfInputSocialMedia.select(ColsSeq.map(col): _*)
      val dfRenamed1: DataFrame = dfInputOldNames.toDF(renamedColsSeq: _*)

      dfRenamed1.show()

      //create a new column
      //1st way
      val dfCreateNewColumns: DataFrame = dfInputSocialMedia
        .withColumn("ShortNames", to_date(col("event_timestamp")))

      dfCreateNewColumns.show()

      //2nd way
      val dfSelectFOrNewColumn = dfInputSocialMedia.select(col("*"),
        expr("case when redirection_source = 'Facebook' then 'FB' " +
          "when redirection_source = 'Twitter'  or  redirection_source = 'twitter' then 'Twitter' " +
          "when redirection_source = 'Instagram' then 'Insta/IG' " +
          "else 'Pinterest' end").alias("ShortForms"))

      dfSelectFOrNewColumn.show()


      //3rd way
      val dfDistinct : DataFrame = dfCreateNewColumns.select(col("*")).distinct()
      dfDistinct.show()

      /* no. of columns */
      val colCount : Int = dfCreateNewColumns.columns.length
      println("no. of columns = " + colCount)


      /* regex_replace */

      val dfReplaced : DataFrame = dfInputSocialMedia
        .withColumn("event_timestamp",regexp_replace(col("event_timestamp"),"Z"," "))

      dfReplaced.show(false)

      /* na.replace */
      val dfNAReplace : DataFrame = dfInputSocialMedia.na.replace(dfInputSocialMedia.columns, Map(" " -> null)) //makes null
      //Map("Facebook" -> null))
      dfNAReplace.show()

      val dfNAFill : DataFrame = dfInputSocialMedia.na.fill("no value")
      dfNAFill.show()

      /* Window */

      val windowRedSource = Window.partitionBy("redirection_source").orderBy("event_timestamp")

      /*lag ,lead and while otherwise */

      val dfLLWO : DataFrame = dfInputSocialMedia
        .orderBy(col("event_timestamp").desc) //desc_nulls_first
        .withColumn("visitor_change_status",
          when(col("visitor_id")===lag(col("visitor_id")
            // when(col("visitor_id")===lead(col("visitor_id")
            ,1).over(windowRedSource) , "yes")
            .otherwise("no"))

      dfLLWO.show()



    }}