import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.expressions.aggregate._
import org.apache.spark.sql.expressions._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import com.google.common.collect.ImmutableMap;

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
     val select_node1 = nodes_df.groupBy("Node2").agg(count("Node2") as "num_neighbours")

    select_node1.write.format("csv").option("mode", "overwrite").save("df11")
   }

   def answerQ2(session: SparkSession, infile1: String, infile2: String,  outfile: String): Unit = {
      //output file must be saved in csv format
      val read_inf1 = session.read.format("parquet").option("inferSchema", "true").load("hdfs://" + infile1)
      val read_inf2 = session.read.format("parquet").option("inferSchema", "true").load("hdfs://" + infile2)
      val df1 = read_inf1.groupBy("medallion", "hack_license").agg(count("hack_license") as "counts_in_jan").select("medallion", "hack_license", "counts_in_jan")
      val df2 = read_inf2.groupBy("medallion", "hack_license").agg(count("hack_license") as "counts_in_feb").select("medallion", "hack_license", "counts_in_feb")
      val df = df1.join(df2, Seq("medallion", "hack_license"), "outer")
      val remove_nas = df.na.fill(0)
      val ans = remove_nas.select("*").where(df("counts_in_feb")>df("counts_in_jan"))
      ans.write.format("csv").option("header", "true").option("mode", "overwrite").save("par")

   }
}
