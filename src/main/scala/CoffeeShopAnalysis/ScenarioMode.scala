package CoffeeShopAnalysis

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.rdd.RDD
import org.apache.log4j.Level
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.types.StructField
//import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.hive.test.TestHive.createDataFrame

object ScenarioMode {
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)
//  val sc = new SparkContext()
  /* Case classes for Mirroring method */
  case class bevBranchA(beverage: String, branch: String)
  case class bevBranchB(beverage: String, branch: String)
  case class bevBranchC(beverage: String, branch: String)
  case class bevConscountA(beverage: String, branch: String)
  case class bevConscountB(beverage: String, branch: String)
  case class bevConscountC(beverage: String, branch: String)

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
    println("Top 5 most consumed beverages in Branch 1:")
    println("+" + ("=" * 49) + "+")
    spark.sql("SELECT DISTINCT c.beverage, c.conscount FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE b.branch = 'Branch1' ORDER BY c.conscount desc").show(1)
    println("+" + ("=" * 49) + "+")
    /* ~ Lease Consumed beverages for Branch 2 ~ */
    println("Top 5 least consumed beverages in Branch 2:")
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
    spark.sql("SELECT * FROM bev_branches_partitioned WHERE branch = 'Branch4' OR branch = 'Branch7'").show(150)
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
    import spark.implicits._
    //  spark.sql("SELECT DISTINCT beverage, branch FROM bev_branches ORDER BY branch asc").show(200)
    //    spark.sql("SELECT max(c.conscount) FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage").show()
    //    spark.sql("SELECT DISTINCT b.branch, c.beverage FROM bev_branches b JOIN bev_conscount c ON b.beverage=c.beverage WHERE c.conscount = 1052 ORDER BY b.branch asc").show(150)

    /* declaration of RDDs */
    val branchARDD = spark.sparkContext.textFile("input/Bev_BranchA.txt")
    val branchBRDD = spark.sparkContext.textFile("input/Bev_BranchB.txt")
    val branchCRDD = spark.sparkContext.textFile("input/Bev_BranchC.txt")
    val conscountARDD = spark.sparkContext.textFile("input/Bev_BranchA.txt")
    val conscountBRDD = spark.sparkContext.textFile("input/Bev_BranchB.txt")
    val conscountCRDD = spark.sparkContext.textFile("input/Bev_BranchC.txt")

    /* Conversion of RDDs to DFs by Mirroring Method */
    val branchRDDA2 = branchARDD.map(_.split(","))
    val branchADF = branchRDDA2.map(attributes => bevBranchA(attributes(0), attributes(1).trim)).toDF()
    branchADF.show()

    //    println("+" + ("-" * 49) + "+")
    val branchRDDB2 = branchBRDD.map(_.split(","))
    val branchBDF = branchRDDB2.map(attributes => bevBranchB(attributes(0), attributes(1).trim)).toDF()
    branchBDF.show()

    //    println("+" + ("-" * 49) + "+")
    val branchRDDC2 = branchCRDD.map(_.split(","))
    val branchCDF = branchRDDC2.map(attributes => bevBranchC(attributes(0), attributes(1).trim)).toDF()
    branchCDF.show()

//    println("+" + ("-" * 49) + "+")
    val conscountARDD2 = conscountARDD.map(_.split(","))
    val conscountADF = conscountARDD2.map(attributes => bevConscountA(attributes(0), attributes(1).trim)).toDF()
    conscountADF.show()

//    println("+" + ("-" * 49) + "+")
    val conscountBRDD2 = conscountBRDD.map(_.split(","))
    val conscountBDF = conscountBRDD2.map(attributes => bevConscountB(attributes(0), attributes(1).trim)).toDF()
    conscountBDF.show()

//    println("+" + ("-" * 49) + "+")
    val conscountCRDD2 = conscountCRDD.map(_.split(","))
    val conscountCDF = conscountCRDD2.map(attributes => bevConscountC(attributes(0), attributes(1).trim)).toDF()
    conscountCDF.show()


    /* Creation of bev_branch RDDs */


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
        scenario6(spark: SparkSession)

  }
}
