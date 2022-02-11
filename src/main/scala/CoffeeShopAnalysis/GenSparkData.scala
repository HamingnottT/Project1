package CoffeeShopAnalysis

import CoffeeShopAnalysis.MainPortal.spark
import org.apache.spark.sql.SparkSession
import scala.io.StdIn._

object GenSparkData extends App{

  def listAllBranches: Unit ={
    val m = CoffeeShopAnalysis.MainPortal

    println("+" + ("=" * 49) + "+")
    println("Returning list of all our active branches:")
    println("+" + ("=" * 49) + "+")

    m.spark.sql("SELECT DISTINCT b.branch FROM bev_branches b join bev_conscount c on b.beverage=c.beverage ORDER BY b.branch asc").show()

    val endInputScenario = readLine("Return to menu [y/n]? ").toLowerCase

    endInputScenario match {
      case "y" => m.main(args: Array[String])
      case "n" => println("\nEnding program...")
    }
  }

  def scenarioMode: Unit ={
    val m = CoffeeShopAnalysis.MainPortal
    val s = CoffeeShopAnalysis.ScenarioMode

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
}
