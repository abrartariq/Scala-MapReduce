package Jobs

import org.apache.hadoop.io.{IntWritable, LongWritable}
import org.slf4j.LoggerFactory
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.{  MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}

import scala.jdk.CollectionConverters.*
import java.{lang, util}
import java.time.LocalTime
import java.util.regex.Pattern
import HelperUtils.CreateLogger

import java.io.IOException

object MRJob1 {

  //  Creating Logger
  val logger = CreateLogger(getClass)

  //  Reading Values from Application Conf
  val cConfig = ConfigFactory.load("application.conf")

  //  Mapper Class for Job 1
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]{
    //  Reading Config Specs For Respective Job
    val mPattern: Pattern = Pattern.compile(cConfig.getString("evValues.mPattern"))       // Message Regex Pattern
    val lPattern: Pattern = Pattern.compile(cConfig.getString("evValues.lPattern"))       // Log Regex Pattern
    val startTime = cConfig.getString("evValues.MR1.StartTime")                           // Start Time Constraint For Logs
    val endTime = cConfig.getString("evValues.MR1.EndTime")                               // End Time Constraint For Logs

    //  This method matches the log message to the log matcher and then checks
    //  if the time of log message lies within the interval or not
    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit ={
      logger.info("Inside MR 1: Mapper")

      // Check if the log message matches the log pattern regex
      val lineMatcher = lPattern.matcher(value.toString)
      if (lineMatcher.matches()){
        val msgMatcher = mPattern.matcher(lineMatcher.group(5))
        val lineTimeStamp = lineMatcher.group(1)
        logger.info(s"Inside MR 1: Mapper$lineTimeStamp")
        // Check the Time Window and msg Pattern
        if (startTime <= lineTimeStamp && lineTimeStamp <= endTime  && msgMatcher.matches()){
          output.collect(new Text(lineMatcher.group(3)), new IntWritable(1))
        }
      }
    }
  }
  // Reducer Class For Job 1
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]{

    // This method get the input as key value pair (outputs of mapper group together)
    // Sums all values of "values" together and outputs the pair
    @throws[IOException]
    @throws[InterruptedException]
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit ={
      logger.info("Inside MR 1: Reducer")
      val sum = values.asScala.reduce((valueOne, valueTwo) => IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
    }
  }

}



