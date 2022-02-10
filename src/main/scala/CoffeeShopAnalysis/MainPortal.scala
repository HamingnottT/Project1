/* Welcome to my second big data project for Revature
* this is a demonstration of how Spark & Hive can be utilized
* through Scala to manipulate and interpret data */

package CoffeeShopAnalysis
import scala.io.StdIn._
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.{Level, Logger}

object MainPortal{

  //Disables INFO logs for clean presentation
  Logger.getLogger("org").setLevel(Level.OFF)
  Logger.getLogger("akka").setLevel(Level.OFF)

  // Establishes Spark connection publicly - Queries on GenSparkData can use this
  System.setProperty("hadoop.home.dir", "C:\\winutils")
  val spark = SparkSession
    .builder()
    .appName("CoffeeShopAnalysis")
    .config("spark.master", "local")
    .enableHiveSupport()
    .getOrCreate()
  spark.sparkContext.setLogLevel("ERROR")

  // Shortens import variables
  val g = CoffeeShopAnalysis.GenSparkData


  def main(args: Array[String]): Unit = {

    println("+" + ("=" * 49) + "+" +
      s"""\nWelcome user to our Coffee Shop database!
         |Below you will find our program's main directory:
         |""".stripMargin + "+" + ("=" * 49) + "+")

    println(
      s"""\nChoose an option below:\n
         |1. List all branches in region
         |2. Show total consumer count
         |3. Show most consumed beverages
         |4. Beverage availability and branch comparisons
         |5. Add comment
         |6. Future predictions
         |7. Scenario Mode
         |8. Cancel & Exit
         |""".stripMargin)

    println("Input option here:")
    var userInput = readInt()

    userInput match {
      case 1 => println("TEST 1")
      case 2 => println("TEST 2")
      case 3 => println("TEST 3")
      case 4 => println("TEST 4")
      case 5 => println("TEST 5")
      case 6 => g.futureQuery
      case 7 => g.scenarioMode
      case 8 => println("\nEnding program...")
      case _ => println("Invalid response - please try again.")
    }


    //Creation of the SQL data tables from GenSparkData
//    CoffeeShopAnalysis.GenSparkData.createDatabase(spark: SparkSession)
//    CoffeeShopAnalysis.GenSparkData.scenario1(spark: SparkSession)
//    CoffeeShopAnalysis.GenSparkData.scenario2(spark: SparkSession)


//      Testing.P1QueryTEST.scenario1(spark: SparkSession)

  }
}
