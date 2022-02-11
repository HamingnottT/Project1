package CoffeeShopAnalysis

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.log4j.Level
import org.apache.log4j.{Level, Logger}

object ScenarioMode {
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
    /* ~ Total consumers for Branch 1 ~ */
    println("+" + ("=" * 49) + "+")
    println("What is the total number of consumers for Branch1?")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT sum(c.conscount) FROM bev_branches b join bev_conscount c on b.beverage=c.beverage where b.branch = 'Branch1'").show()

    /* ~ Total consumers for Branch 2 ~ */
    println("+" + ("=" * 49) + "+")
    println("What is the total number of consumers for Branch2?")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT sum(c.conscount) FROM bev_branches b join bev_conscount c on b.beverage=c.beverage where b.branch = 'Branch2'").show()

    /* ~ Combined table of Bev_Branches and Bev_Conscount ~ */
    println("+" + ("=" * 49) + "+")
    println(s"""Combined table of Branches and Consumer Count
               |by Beverage Name:""".stripMargin)
    println("+" + ("=" * 49) + "+")
    spark.sql("select DISTINCT b.branch, c.beverage, c.conscount from bev_branches b join bev_conscount c on b.beverage = c.beverage ORDER BY b.branch desc").show(150)
    println("+" + ("=" * 49) + "+")
  }

  def scenario2(spark: SparkSession): Unit ={
    /* ~ Most Consumed beverages for Branch 1 ~ */
    println("+" + ("=" * 49) + "+")
    println("Top most consumed beverages in Branch 1:")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT c.beverage, c.conscount FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch1' ORDER BY c.conscount desc").show(1)
    println("+" + ("=" * 49) + "+")

    /* ~ Lease Consumed beverages for Branch 2 ~ */
    println("Top least consumed beverages in Branch 2:")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT c.beverage, c.conscount FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch2' ORDER BY c.conscount asc").show(1)

    /* ~ Average Consumed beverages for Branch 2 ~ */
    println("+" + ("=" * 49) + "+")
    println("Average consumed beverage in Branch 2:")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT c.beverage, ROUND(AVG(c.conscount)) avg_conscount FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch2' GROUP BY c.beverage").show(1)
  }

  def scenario3(spark: SparkSession): Unit ={
    /* ~ Beverages available in branches 9, 8, and 1 ~ */
    /*
    println("+" + ("=" * 49) + "+")
    println("beverages available in branches 9, 8, and 1:")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT b.branch, c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch9' OR b.branch = 'Branch8' OR b.branch = 'Branch1' ORDER BY b.branch desc").show(40)
    */

    /* ~ Beverage availability on Branch 10 ~ */
    println("+" + ("=" * 49) + "+")
    println("beverages available on Branch10")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch10'").show(40)

    /* ~ Beverage availability on Branch 9 ~ */
    println("+" + ("=" * 49) + "+")
    println("beverages available on Branch9")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch9'").show(40)

    /* ~ Beverage availability on Branch 8 ~ */
    println("+" + ("=" * 49) + "+")
    println("beverages available on Branch8")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch8'").show(40)
    println("+" + ("=" * 49) + "+")

    /* ~ Beverage availability on Branch 1 ~ */
    println("beverages available on Branch1")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch1'").show(40)
    println("+" + ("=" * 49) + "+")

    /* ~ Common beverages on Branch 4 & 7 [INNER JOIN]  ~ */
    println("beverages available on Branch 4 and 7")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT b.branch, c.beverage FROM bev_branches b INNER JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch4' OR b.branch = 'Branch7' ORDER BY b.branch asc").show(150)
  }

  def scenario4(spark: SparkSession): Unit ={
    /* ~ Create new table partitioned by branch from existing other tables ~ */
    //    spark.sql("CREATE TABLE IF NOT EXISTS bev_branches_partitioned(beverage String) PARTITIONED BY (branch String)")
    //    spark.sql("set hive.exec.dynamic.partition.mode=nonrestrict")

    /* ~ Loads data from other tables into partitioned table ~ */
    //    spark.sql("INSERT OVERWRITE TABLE bev_branches_partitioned PARTITION(branch) SELECT beverage,branch FROM bev_branches")

    /* ~  Showcase of partitioned table calling Branch 4 & 7 ~ */
    println("+" + ("=" * 49) + "+")
    println(
      s"""NOTE: Partitioned table is a table created based on the joined main tables.
         |This showcase calls branch 4 and 7 like scenario 3.""".stripMargin)
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT * FROM bev_branches_partitioned WHERE branch = 'Branch4' OR branch = 'Branch7'").show(150)
    spark.sql("Describe formatted bev_branches_partitioned").show(50)
  }

  def scenario5(spark: SparkSession): Unit ={
    /* ~ Adds test notes and comments ~ */
    //    spark.sql("ALTER TABLE bev_branches_partitioned SET TBLPROPERTIES('comment' = 'Partitioned by branch.')")
    //    spark.sql("ALTER TABLE bev_branches_partitioned SET TBLPROPERTIES('note' = 'TEST note.')")

    /* ~ Displays information on partitioned table ~ */
    println("+" + ("=" * 49) + "+")
    println("Information on partitioned table:")
    println("+" + ("=" * 49) + "+")
    spark.sql("SHOW TBLPROPERTIES bev_branches_partitioned").show
    spark.sql("Describe formatted bev_branches_partitioned").show(50)

    /* ~ Displays information on the two main tables used in this project for comparison ~ */
    println("+" + ("=" * 49) + "+")
    println("Other tables used [source]:")
    println("+" + ("=" * 49) + "+")
    spark.sql("Describe formatted bev_branches").show
    spark.sql("Describe formatted bev_conscount").show
    println("+" + ("=" * 49) + "+")

    /* ~ Deletes a row from a table - for comparison there is a before and after query on row count ~ */
    //    println("Count of total beverages in branch 4:")
    //    println("~ Before row deletion ~")
    //    spark.sql("SELECT count(beverage) FROM bev_branches_partitioned WHERE branch = 'Branch4'").show()
    //    spark.sql("DELETE FROM bev_branches_partitioned WHERE branch = 'Branch4' AND beverage = 'SMALL_cappuccino'")
    //    println("~ After row deletion ~")
    //    spark.sql("SELECT count(beverage) FROM bev_branches_partitioned WHERE branch = 'Branch4'").show()
  }

  def scenario6(spark: SparkSession): Unit ={
    /* The purpose of scenario 6 is a macro glimpse of Coffee Shop Inc.
     * in terms of beverage variety/availability and how it relates to branch
     * performance. */

    /* Declares lazy variables to populate and write out CSV for Excel Use
    * Lazy was chosen prevent unnecessary extra CPU load when compiling */
    lazy val pop_beverages = spark.sql("SELECT DISTINCT b.branch, c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE c.conscount = 1052 ORDER BY b.branch asc")
    lazy val avail_bev = pivot1src.groupBy("b.branch").agg(count("c.beverage")).sort("b.branch")
    lazy val totalSales = pivot1src.groupBy("c.beverage").pivot("b.branch").agg(count("c.conscount")).sort("c.beverage")
    lazy val combinedTable = spark.sql("select b.branch, c.beverage, c.conscount from bev_branches b join bev_conscount c on b.beverage = c.beverage ORDER BY b.branch desc")
    lazy val pivot1src = spark.sql("SELECT b.branch, c.beverage, c.conscount FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage")
    val tsvWithHeaderOptions: Map[String, String] = Map(
//      ("delimiter", "\t"), // Uses "\t" delimiter instead of default ","
      ("header", "true"))
    /* ~ Output most popular beverages by branch in csv - for Excel use ~ */
    /*
    avail_bev.coalesce(1)         // Writes to a single file
      .write
      .mode(SaveMode.Overwrite)
      .options(tsvWithHeaderOptions)
      .csv("output/avail_bev")
    */

    println("+" + ("=" * 49) + "+")
    println("Absolute maximum consumer sales per beverage:")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT max(c.conscount) FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage").show()

    println("+" + ("=" * 49) + "+")
    println("Total number of beverages available over entire region:")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT count(b.beverage) FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage").show

    println("+" + ("=" * 49) + "+")
    println("Total consumer sales in all branches:")
    println("+" + ("=" * 49) + "+")
    pivot1src.groupBy("b.branch").agg(sum("c.conscount")).sort("b.branch").show(50)

    println("+" + ("=" * 49) + "+")
    println("Total number of beverages available per branch:")
    println("+" + ("=" * 49) + "+")
    pivot1src.groupBy("b.branch").agg(count("c.beverage")).sort("b.branch").show(50)

    println("+" + ("=" * 49) + "+")
    println("Individual beverage sales per branch:")
    println("+" + ("=" * 49) + "+")
    pivot1src.groupBy("c.beverage").pivot("b.branch").agg(count("c.conscount")).sort("c.beverage").show(50)
  }

  def main(args: Array[String]): Unit = {
    /* Unused by MainPortal - this main method is still required for program to work
    *  Use this function for debugging */

    System.setProperty("hadoop.home.dir", "C:\\winutils")

    val spark = SparkSession
      .builder()
      .appName("CoffeeShopAnalysis")
      .config("spark.master", "local")
      .enableHiveSupport()
      .getOrCreate()

    /* ~ Creates the spark-warehouse database ~ */
    //    createDatabase(spark: SparkSession)

    /* ~ Uncomment for debug testing ~ */
//        scenario1(spark: SparkSession)
//        scenario2(spark: SparkSession)
//        scenario3(spark: SparkSession)
//        scenario4(spark: SparkSession)
//        scenario5(spark: SparkSession)
//        scenario6(spark: SparkSession)
  }
}
