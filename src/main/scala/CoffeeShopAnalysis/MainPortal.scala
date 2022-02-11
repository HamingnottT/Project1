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
      s"""\nWelcome user to Coffee Shop Inc. database!
         |Below you will find our program's main directory:
         |""".stripMargin + "+" + ("=" * 49) + "+")

    println(
      s"""\nChoose an option below:\n
         |1. List all branches in region
         |2. Scenario Mode
         |3. Cancel & Exit
         |""".stripMargin)

    println("Input option here:")
    var userInput = readInt()

    userInput match {
      case 1 => g.listAllBranches
      case 2 => g.scenarioMode
      case 3 => println("\nEnding program...")
      case _ => println("Invalid response - please try again.")
    }
  }
}
