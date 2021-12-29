import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._


object Spark_Assignment1 extends App{
  System.setProperty("hadoop.home.dir", "C:/null")
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
  sparkConf.set("spark.app.name", "newcolumn")
  sparkConf.set("spark.master", "local[2]")

  val spark = SparkSession.builder()
    .config(sparkConf)
    .getOrCreate()


  val rdd1 =spark.sparkContext.textFile("C:/bhavani/Userdata.csv")
 val rdd2 = spark.sparkContext.textFile("C:/bhavani/Transactions_data.csv")
  val with_header = rdd1.first()
   val without_header = rdd1.filter(x => x!=with_header)
  val rdd1_trans = without_header
                            .map(x => x.split(","))
                                  .map(x => x(3))
                                  .distinct()
  //counting distinct locations
  println("====uniquelocations======")
  rdd1_trans.foreach(println)

  val with_header1 = rdd2.first()
  val without_header2 = rdd2.filter(x => x!=with_header1)
  val rdd_trans = without_header2.map(x => {
    val fields = x.split(",")
    val product = fields(1).toInt
    val user = fields(2).toInt
    val price = fields(3).toInt
    (user,product,price)
  })
  println("======products brought by each user======")
  val rdd_trans2 = rdd_trans.map(x => (x._1, x._2))
   val new_rdd =  rdd_trans2.map(x => (x._1,(x._2,1)))
  val trans_rdd = new_rdd.reduceByKey((x,y) => (x._1 + y._2 , x._2 + y._2))
  val aftr_tran = trans_rdd.map(x => (x._1,x._2._2)).sortBy(x => x._2)

  for(result2<-aftr_tran){
    val user_id = result2._1
    val product_id = result2._2
    println(s"The userid $user_id has brought $product_id products" )
  }


  val split_rdd = without_header2.map(x => x.split(","))
    .map(x => (x(2).toInt,x(3).toInt))
    .map(x => (x._1,(x._2,1)))
    .reduceByKey((x,y) => (x._1 + y._1 ,x._2 + y._2) )
    .map( x=> (x._1,x._2._1))
    .sortBy(x => x._1)


  println("======count of sales per each user price_value=======")
for(result<-split_rdd){
  val user_id = result._1
  val price_value = result._2
  println(s" The userid $user_id has made the purchase of Rps.$price_value")
}



}