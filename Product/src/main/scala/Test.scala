import org.apache.spark
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}


object Test{
  def main(args: Array[String]): Unit = {
  val sparkSession = org.apache.spark.sql.SparkSession.builder.master("local").appName("Sparkassignment-Training").getOrCreate
//    val data1 = sparkSession.sparkContext.parallelize(Seq(("sun",1),("mon",2),("tue",3), ("wed",4),("thus",5)))
//    data1.foreach(println)

    //Create dataFrame from Seq:
    //Using createDataframe() method:
//        val data = Seq(
//          Row(8, "bat"),
//          Row(64, "mouse"),
//          Row(-27, "horse")
//        )
//
//        val schema = StructType(
//          List(
//            StructField("number", IntegerType, true),
//            StructField("word", StringType, true)
//          )
//        )
//
//        val dfTest = sparkSession.createDataFrame( sparkSession.sparkContext.parallelize(data), schema )
//    dfTest.printSchema()
//    dfTest.show()


//    Create dataFrame from file:
//    Read csv File without schema defination:

    val Schema = StructType(Array(
      StructField("Name",StringType,true),
      StructField("Dept",StringType,true)
    ))

    val df_schema = sparkSession.read.schema(Schema).options(Map("header"->"true")).csv("C:/Users/harsh/IdeaProjects/NewScalaProject/data/spark_assignment.csv")
      df_schema.show(false)
    df_schema.printSchema()


  }
}
