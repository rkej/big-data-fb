import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

object Lab {
   def main(args: Array[String]) = {
      val q1_infile = "/ds410/facebook"
      val q2_infile_a = "/ds410/tripdata/trip_data_1.parquet"
      val q2_infile_b = "/ds410/tripdata/trip_data_2.parquet"
      val q1_outfile = "q1_result.csv"
      val q2_outfile = "q2_result.csv"
      val session = getSession()


      answerQ1(session, q1_infile, q1_outfile)
      answerQ2(session, q2_infile_a, q2_infile_b, q2_outfile)
   }


   def getSession() = {
     //you need to fill this in to return a SparkSession (in the spark shell, this is the 'spark' variable
     // in the slides it is called 'session'
    val session = SparkSession.builder().getOrCreate()
    session
   }

   def answerQ1(session: SparkSession, infile: String, outfile: String): Unit = {
      //when reading input, you must give the schema, no inferSchema allowed
      //output file must be saved in csv format
     val mySchema = StructType(Array(StructField("Node1", StringType, false), StructField("Node2", StringType, false)))
     val df = session.read.format("csv").option("delimiter", "\t").schema(mySchema).load("hdfs://"+infile)

     val node1_df = df.drop("Node1")
     val node2_df = df.drop("Node2")
     val nodes_df = node1_df.union(node2_df)
     val select_node1 = nodes_df.groupBy("Node2").agg(countDistinct("Node2") as "num_neighbours")

     select_node1.write.format("csv").option("mode", "overwrite").save("df10")
   }

   def answerQ2(session: SparkSession, infile1: String, infile2: String,  outfile: String): Unit = {
      //output file must be saved in csv format
      val read_inf1 = session.read.parquet("hdfs://" + infile1)
      read_inf1.write.format("csv").option("mode", "overwrite").save("par")
   }
}
