package Jobs

import org.apache.hadoop.io.{IntWritable, LongWritable}
import org.slf4j.LoggerFactory
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reducer, Reporter}

import scala.jdk.CollectionConverters.*
import java.{lang, util}
import java.time.LocalTime
import java.util.regex.Pattern
import HelperUtils.CreateLogger

import java.io.IOException

object MRJob4 {


  //  Creating Logger
  val logger = CreateLogger(getClass)

  //Reading Values from Application Conf
  val cConfig = ConfigFactory.load("application.conf")
  val mPattern: Pattern = Pattern.compile(cConfig.getString("evValues.mPattern"))       // Message Regex Pattern
  val lPattern: Pattern = Pattern.compile(cConfig.getString("evValues.lPattern"))       // Log Regex Pattern
  // This method concat two strings with the delimiter
  def combineString(valOne: String, valTwo: String): String ={
    valOne.concat("|").concat(valTwo)
  }

  // This method deconcat two strings with the delimiter
  def decombineString(combinedVal: String): (String, String) ={
    combinedVal.split("\\" + "|") match
      case Array(a, b) => (a, b)
      case _ => ("0", "0")
  }
  //  Map Class For Job 4
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, Text] {

    // This method matches the log message to the log pattern, if matches, writes to output with log message type as key and log message length and string instance length encoded together as value
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, Text], reporter: Reporter): Unit = {
      val lineMatcher = lPattern.matcher(value.toString)
      if (lineMatcher.matches()){
        output.collect(new Text(lineMatcher.group(3)), new Text(combineString( lineMatcher.group(0).length.toString, lineMatcher.group(5).length.toString )))
      }
  }}

  //Reducer Class for Job 4
  class Reduce extends MapReduceBase with Reducer[Text, Text, Text, IntWritable]{

    def getMaxChar(text: Text, previousMax: (Int, Int)): (Int, Int) ={
      val (logMsgLength: String, patternInstanceLength: String) = decombineString(text.toString)
      if (patternInstanceLength.toInt > previousMax._2){
        (logMsgLength.toInt, patternInstanceLength.toInt)
      } else{
        previousMax
      }
    }

    // This method get the input as key value pair, which are the outputs of mapper group together
    // Finds the max of values for a single key and then outputs key, value pair
    @throws[IOException]
    @throws[InterruptedException]
    override def reduce(key: Text, values: util.Iterator[Text], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {
      logger.info("reducer function called")
      val (maxCharLength, _) = values.asScala.foldRight((0, 0))(getMaxChar)
      output.collect(key, IntWritable(maxCharLength))
    }

  }

}
