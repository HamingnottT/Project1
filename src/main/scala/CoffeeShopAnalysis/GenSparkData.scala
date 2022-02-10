package CoffeeShopAnalysis

import CoffeeShopAnalysis.MainPortal.spark
import org.apache.spark.sql.SparkSession
import scala.io.StdIn._

object GenSparkData extends App{

  def createDatabase(spark: SparkSession): Unit ={
    /* ~ Create Spark SQL tables - loading Branch & conscount respectively ~ */
//        spark.sql("create table if not exists bev_branches(beverage String,branch String) row format delimited fields terminated by ','")
//        spark.sql("create table if not exists bev_conscount(beverage String,conscount Int) row format delimited fields terminated by ','")
    /* ~ Load Bev Branch data into bev_branches ~ */
//        spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchA.txt' INTO TABLE bev_branches")
//        spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchB.txt' INTO TABLE bev_branches")
//        spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_BranchC.txt' INTO TABLE bev_branches")
    /* ~ Load Bev Conscount data into bev_conscount ~ */
//        spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountA.txt' INTO TABLE bev_conscount")
//        spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountB.txt' INTO TABLE bev_conscount")
//        spark.sql("LOAD DATA LOCAL INPATH 'input/Bev_ConscountC.txt' INTO TABLE bev_conscount")
  }

  def scenarioMode: Unit ={
    val m = CoffeeShopAnalysis.MainPortal
    val s = CoffeeShopAnalysis.ScenarioMode
    val t = Testing.P1QueryTEST

    println("+" + ("=" * 49) + "+" +
      s"""\nScenario Mode chosen. This lists out Project 1's
         |problems by scenario.
         |""".stripMargin + "+" + ("=" * 49) + "+")

    println(
      s"""\nChoose an option below:\n
         |1. Scenario 1
         |2. Scenario 2
         |3. Scenario 3
         |4. Scenario 4
         |5. Scenario 5
         |6. Scenario 6
         |7. Back to main menu
         |8. Cancel & Exit
         |""".stripMargin)

//    Testing.P1QueryTEST.scenario1(spark: SparkSession)

    println("Input option here:")
    var userInputScenario = readInt()

    userInputScenario match {
      case 1 => s.scenario1(spark: SparkSession)
      case 2 => s.scenario2(spark: SparkSession)
      case 3 => s.scenario3(spark: SparkSession)
      case 4 => s.scenario4(spark: SparkSession)
      case 5 => s.scenario5(spark: SparkSession)
      case 6 => s.scenario6(spark: SparkSession)
      case 7 => m.main(args: Array[String])
      case 8 => println("\nEnding program...")
      case _ => println("Invalid response - please try again.")
    }

    /* if conditional is buggy if choosing to go back to main menu
     * variable userInputScenario retains original integer value
     * pending fix */

    if (userInputScenario != 8) {
      val endInputScenario = readLine("Return to menu [y/n]? ").toLowerCase

      endInputScenario match {
        case "y" => m.main(args: Array[String])
        case "n" => println("\nEnding program...")
      }
    }
  }

  def futureQuery: Unit = {
    val m = CoffeeShopAnalysis.MainPortal
    val t = Testing.P1QueryTEST
    println("+" + ("=" * 49) + "+" +
      s"""\nFuture predictions chosen. This is a hypothetical
         |analysis of what could happen in the next year, and
         |how it may affect the organization's future goals.
         |""".stripMargin + "+" + ("=" * 49) + "+")

    println(
      s"""\nChoose an option below:\n
         |1. Scenario 1
         |2. Scenario 2
         |3. Scenario 3
         |4. Scenario 4
         |5. Scenario 5
         |6. Scenario 6
         |7. Back to main menu
         |8. Cancel & Exit
         |""".stripMargin)

    println("Input option here:")
    var userInputFuture = readInt()

    userInputFuture match {
      case 1 => println("TEST 1")
      case 2 => println("TEST 2")
      case 3 => println("TEST 3")
      case 4 => println("TEST 4")
      case 5 => println("TEST 5")
      case 6 => println("TEST 6")
      case 7 => m.main(args: Array[String])
      case 8 => println("\nEnding program...")
      case _ => println("Invalid response - please try again.")
    }
  }

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

    /*
    println("+" + ("=" * 49) + "+")
    println(s"""Combined table of Branches and Consumer Count
               |by Beverage Name:""".stripMargin)
    println("+" + ("=" * 49) + "+")
    spark.sql("select * from bev_branches b join bev_conscount c on b.beverage = c.beverage").show(150)
    println("+" + ("=" * 49) + "+")
     */
  }

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

}
