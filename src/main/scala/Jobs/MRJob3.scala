package Jobs

import HelperUtils.CreateLogger
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.hadoop.mapred.*
import org.slf4j.LoggerFactory

import java.io.IOException
import java.time.LocalTime
import java.util.regex.Pattern
import java.{lang, util}
import scala.jdk.CollectionConverters.*


object MRJob3 {

  //  Creating Logger
  val logger = CreateLogger(getClass)

  //  Reading Config Specs For Respective Job
  val cConfig = ConfigFactory.load("application.conf")
  val mPattern: Pattern = Pattern.compile(cConfig.getString("evValues.mPattern"))       // Message Regex Pattern
  val lPattern: Pattern = Pattern.compile(cConfig.getString("evValues.lPattern"))       // Log Regex Pattern

  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]{
    //  This method matches the log message to the log matcher and then collects them by Type
    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit ={
      logger.info("Inside MR 3: Mapper")

      val lineMatcher = lPattern.matcher(value.toString)
      if (lineMatcher.matches()){
        output.collect(new Text(lineMatcher.group(3)), new IntWritable(1))
      }
    }
  }



  // Reducer Class For Job 3
  class Reduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]{
    // This method get the input as key value pair (outputs of mapper group together)
    // Sums all values of "values" together and outputs the pair
    @throws[IOException]
    @throws[InterruptedException]
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit ={
      logger.info("Inside MR 3: Reducer")
      val sum = values.asScala.reduce((valueOne, valueTwo) => new IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
    }
  }

}



