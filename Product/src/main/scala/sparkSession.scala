object sparkSession {
  def main(args: Array[String]): Unit = {
  val sparkSession = org.apache.spark.sql.SparkSession.builder.master("local").appName("Sparkassignment-Training").getOrCreate
}}
