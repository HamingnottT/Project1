/* Main archive of all problem scenarios used for testing queries */

package Testing

import java.io.File
import java.io.PrintWriter
import scala.io.Source
import scala.io.StdIn.readLine
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
//Gets rid of annoying red text - Works!
import org.apache.log4j.Level
import org.apache.log4j.{Level, Logger}


object P1QueryTEST {
    Logger.getLogger("org").setLevel(Level.OFF)
    Logger.getLogger("akka").setLevel(Level.OFF)
  //Tables creation - load once then comment out
  def createDatabase(spark: SparkSession): Unit ={
    /* ~ Create Spark SQL tables - loading Branch & conscount respectively ~ */
    //    spark.sql("create table if not exists bev_branches(beverage String,branch String) row format delimited fields terminated by ','")
    //    spark.sql("create table if not exists bev_conscount(beverage String,conscount Int) row format delimited fields terminated by ','")
    /* ~ Load Bev Branch data into bev_branches ~ */
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE bev_branches")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE bev_branches")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE bev_branches")
    /* ~ Load Bev Conscount data into bev_conscount ~ */
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE bev_conscount")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE bev_conscount")
    //    spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE bev_conscount")
  }

  //Count of how many beverages consumed at Branches 1 and 2
  def scenario1(spark: SparkSession): Unit ={
    spark.sql("SELECT sum(c.conscount) FROM bev_branches b join bev_conscount c on b.beverage=c.beverage where b.branch = 'Branch1'")
    println("+" + ("=" * 49) + "+")
    println("What is the total number of consumers for Branch1?")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT sum(c.conscount) FROM bev_branches b join bev_conscount c on b.beverage=c.beverage where b.branch = 'Branch1'").show()
    println("+" + ("=" * 49) + "+")
    println("What is the total number of consumers for Branch2?")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT sum(c.conscount) FROM bev_branches b join bev_conscount c on b.beverage=c.beverage where b.branch = 'Branch2'").show()
    println("+" + ("=" * 49) + "+")
    println(s"""Combined table of Branches and Consumer Count
               |by Beverage Name:""".stripMargin)
    println("+" + ("=" * 49) + "+")
    spark.sql("select DISTINCT b.branch, c.beverage, c.conscount from bev_branches b join bev_conscount c on b.beverage = c.beverage ORDER BY b.branch desc").show(150)
    println("+" + ("=" * 49) + "+")
  }

  //    What is the most consumed beverage on Branch1
  //    What is the least consumed beverage on Branch2
  //    What is the Average consumed beverage of  Branch2
  def scenario2(spark: SparkSession): Unit ={
    println("+" + ("=" * 49) + "+")
    println("Top 5 most consumed beverages in Branch 1:")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT c.beverage, c.conscount FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch1' ORDER BY c.conscount desc").show(1)
    println("+" + ("=" * 49) + "+")
    println("Top 5 least consumed beverages in Branch 2:")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT c.beverage, c.conscount FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch2' ORDER BY c.conscount asc").show(1)
    println("+" + ("=" * 49) + "+")
    println("Average consumed beverage in Branch 2:")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT c.beverage, ROUND(AVG(c.conscount)) avg_conscount FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch2' GROUP BY c.beverage").show(1)
  }

  def scenario3(spark: SparkSession): Unit ={
    println("+" + ("=" * 49) + "+")
    println("beverages available in branches 9, 8, and 1:")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT b.branch, c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch9' OR b.branch = 'Branch8' OR b.branch = 'Branch1' ORDER BY b.branch desc").show(40)
    println("+" + ("=" * 49) + "+")
    println("beverages available on Branch8")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch8'").show(40)
    println("+" + ("=" * 49) + "+")
    println("beverages available on Branch1")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch1'").show(40)
    println("+" + ("=" * 49) + "+")
    println("beverages available on Branch 4 and 7")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT b.branch, c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch4' OR b.branch = 'Branch7' ORDER BY b.branch asc").show(150)
  }

  def scenario4(spark: SparkSession): Unit ={
//    spark.sql("CREATE TABLE IF NOT EXISTS bev_branches_partitioned(beverage String) PARTITIONED BY (branch String)")
//    spark.sql("set hive.exec.dynamic.partition.mode=nonrestrict")
//    spark.sql("INSERT OVERWRITE TABLE bev_branches_partitioned PARTITION(branch) SELECT beverage,branch FROM bev_branches")
    spark.sql("SELECT * FROM bev_branches_partitioned WHERE branch = 'Branch4' OR branch = 'Branch7'").show(150)
  }

  def scenario5(spark: SparkSession): Unit ={
//    spark.sql("ALTER TABLE bev_branches_partitioned SET TBLPROPERTIES('comment' = 'Partitioned by branch.')")
//    spark.sql("ALTER TABLE bev_branches_partitioned SET TBLPROPERTIES('note' = 'TEST note.')")
    println("+" + ("=" * 49) + "+")
    println("Information on partitioned table:")
    println("+" + ("=" * 49) + "+")
    spark.sql("SHOW TBLPROPERTIES bev_branches_partitioned").show
    spark.sql("Describe formatted bev_branches_partitioned").show(50)
    println("+" + ("=" * 49) + "+")
    println("Other tables used [source]:")
    println("+" + ("=" * 49) + "+")
    spark.sql("Describe formatted bev_branches").show
    spark.sql("Describe formatted bev_conscount").show
    println("+" + ("=" * 49) + "+")

//    println("Count of total beverages in branch 4:")
//    println("~ Before row deletion ~")
//    spark.sql("SELECT count(beverage) FROM bev_branches_partitioned WHERE branch = 'Branch4'").show()
//    spark.sql("DELETE FROM bev_branches_partitioned WHERE branch = 'Branch4' AND beverage = 'SMALL_cappuccino'")
//    println("~ After row deletion ~")
//    spark.sql("SELECT count(beverage) FROM bev_branches_partitioned WHERE branch = 'Branch4'").show()

  }

  def scenario6(spark: SparkSession): Unit ={
    import spark.implicits._
//  spark.sql("SELECT DISTINCT beverage, branch FROM bev_branches ORDER BY branch asc").show(200)
//    spark.sql("SELECT max(c.conscount) FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage").show()
//    spark.sql("SELECT DISTINCT b.branch, c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE c.conscount = 1052 ORDER BY b.branch asc").show(150)

    val columns = Seq("language","users_count")
    val data = Seq(("Java", "20000"), ("Python", "100000"), ("Scala", "3000"))
    val rdd = spark.sparkContext.parallelize(data)
    val dfFromRDD1 = rdd.toDF()
    dfFromRDD1.printSchema()
  }

  def main(args: Array[String]): Unit = {
    // create a spark session
    // for Windows

    System.setProperty("hadoop.home.dir", "C:\\winutils")
    val spark = SparkSession
      .builder()
      .appName("P1Test")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    //    spark.sparkContext.setLogLevel("ERROR")
    //    spark.catalog.listDatabases().show(false)
    //    spark.catalog.listTables().show(false)
    //    spark.conf.getAll.mkString("\n")

//    createDatabase(spark: SparkSession)

//    scenario1(spark: SparkSession)
//    scenario2(spark: SparkSession)
//    scenario3(spark: SparkSession)
//    scenario4(spark: SparkSession)
    scenario5(spark: SparkSession)
//    scenario6(spark: SparkSession)

    /*
    val spark = SparkSession.builder()
      .appName("HiveTest5")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()
    println("created spark session")
    spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) USING hive")
    spark.sql("CREATE TABLE IF NOT EXISTS src(key INT, value STRING) ROW FORMAT DELIMITED FIELDS TERMINATED BY ‘,’ STORED AS TEXTFILE")
    spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE src")
    spark.sql("CREATE TABLE IF NOT EXISTS src (key INT,value STRING) USING hive")
    spark.sql("create table newone1(id Int,name String) row format delimited fields terminated by ','");
    spark.sql("LOAD DATA LOCAL INPATH 'input/kv1.txt' INTO TABLE newone1")
    spark.sql("SELECT * FROM newone1").show()
     */
  }
}
