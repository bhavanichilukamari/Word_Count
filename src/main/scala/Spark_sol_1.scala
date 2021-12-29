import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Spark_sol_1 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "spark_df")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()

   val df_1 = spark.read
     .format("csv")
     .option("header",true)
     .option("inferschema",true)
     .option("path","C:/bhavani/Userdata.csv")
     .load()
  df_1.show()
  val df_2 = spark.read
    .format("csv")
    .option("header",true)
    .option("inferschema",true)
    .option("path","C:/bhavani/Transactions_data.csv")
    .load()
  df_2.show()

  val joined_df = df_1.join(df_2,df_1("UserId") === df_2("UserId"),"outer").drop(df_2("UserId"))
  joined_df.show()
println("=====The distinct Locations=====")
  val distinct_count = joined_df.select ("Location") .distinct().show
  println("======The products brought by each user======")
  val count_products = joined_df.groupBy("UserId")
    .agg(expr("count(Product_Id) As Total")).orderBy("UserId")
    .show()

  println("=====purchased amount of each user======")
  val price_count = joined_df.select("UserId","Price","Product Description")
    .groupBy("UserId","Product Description")
    .agg(expr("sum(Price) as Total_spending")).orderBy("UserId")
    .show()
}
