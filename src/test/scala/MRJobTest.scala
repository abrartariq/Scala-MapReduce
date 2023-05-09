
import org.scalatest.funspec.AnyFunSpec
import org.scalatest.funsuite.AnyFunSuite
import org.apache.hadoop.io.{IntWritable, Text}
import org.apache.hadoop.mapred.OutputCollector
import Jobs.MRJob4._
import Jobs.MRJob2._
import Jobs.MapReduceProgram._
import org.scalatest._
import org.scalatest.matchers.should.Matchers.*
import com.typesafe.config.{Config, ConfigFactory}
import scala.io.Source
import java.time.LocalTime
import java.util.regex.Pattern
import java.io.File

class MRJobTest extends AnyFunSuite {


  test("Check Output was Created ") {
    clearOutputDir("src/test/TestOutput")
    executeMRJob3("src/test/TestInput", "src/test/TestOutput")

    assert(File("src/test/TestOutput/Result.csv").exists(), "This Should Return True")
  }

  test("Check if MessageType Count is Correct (e.g DEBUG = 206)"){
    val myLine = {
      val src = Source.fromFile("src/test/TestOutput/Result.csv")
      val line = src.getLines.toList
      src.close
      line
    }

    assert(myLine(0) == "DEBUG,206", "This Should Return True")
    assert(myLine(1) == "ERROR,12", "This Should Return True")
    assert(myLine(2) == "INFO,1410", "This Should Return True")
    assert(myLine(3) == "WARN,378", "This Should Return True")

  }

  test("if Files are cleared for new Future Outputs"){
    clearOutputDir("src/test/TestOutput")
    assert(!File("src/test/TestOutput/part-00000").exists(), "This Should Return True")
  }



  test("combineString should concatenate two strings with a '|' separator") {
    val str1 = "Hello"
    val str2 = "World"
    val combined = combineString(str1, str2)

    assert(combined == "Hello|World", "The combined string should be 'Hello|World'")
  }

  test("decombineString should split a combined string into a tuple of two strings") {
    val combined = "Hello|World"
    val (str1, str2) = decombineString(combined)

    assert(str1 == "Hello", "The first string should be 'Hello'")
    assert(str2 == "World", "The second string should be 'World'")
  }

  test("Conig Read Should be of Type Config") {
    val cConfig = ConfigFactory.load("application.conf")
    assert(cConfig.isInstanceOf[Config])
  }

  test("should extract the log message type from log entry") {
    val logEntry = "13:05:54.942 [scala-execution-context-global-17] DEBUG HelperUtils.Parameters$ - AdEbEe[?J:)K]x!]=N8&qq(,]Q/:@2#O{@t<Ei$9?\\#FO)qG@h"
    val lPattern: Pattern = Pattern.compile(ConfigFactory.load("application.conf").getString("evValues.lPattern"))
    val matcher = lPattern.matcher(logEntry)
    matcher.matches()

    assert(matcher.group(3) == "DEBUG", " Should extract the Correct log message DBUG")
  }

  test("createWindowTime should return the correct start and end times") {
    val cConfig = ConfigFactory.load("application.conf")
    val resetTime: LocalTime = LocalTime.parse(cConfig.getString("evValues.MR2.resetTime"))
    val eTime = "12:30:00"
    val windowMinutes = 15
    val (startTime, endTime) = createWindowTime(eTime, windowMinutes)

    assert(startTime == "12:30:00", "The start time should be '12:30:00'")
    assert(endTime == "12:45:00", "The end time should be '12:45:00'")
  }

  test("createWindowTime should return the correct start and end times even with increasing of Hours") {
    val cConfig = ConfigFactory.load("application.conf")
    val resetTime: LocalTime = LocalTime.parse(cConfig.getString("evValues.MR2.resetTime"))
    val eTime = "12:59:00"
    val windowMinutes = 1
    val (startTime, endTime) = createWindowTime(eTime, windowMinutes)

    assert(startTime == "12:59:00", "The start time should be '12:59:00'")
    assert(endTime == "13:00:00", "The end time should be '13:00:00'")
  }


}
