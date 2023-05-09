package Jobs

import org.apache.hadoop.io.{IntWritable, LongWritable}
import org.slf4j.LoggerFactory
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.io.{IntWritable, LongWritable, NullWritable, Text, WritableComparable, WritableComparator}
import org.apache.hadoop.mapred.{MapReduceBase, Mapper, OutputCollector, Reducer, Reporter, TextInputFormat, TextOutputFormat}

import scala.jdk.CollectionConverters.*
import java.{lang, util}
import java.time.LocalTime
import java.util.regex.Pattern
import HelperUtils.CreateLogger

import java.io.IOException


object MRJob2 {
  //  Creating Logger
  val logger = CreateLogger(getClass)


  //    Reading Config Specs For Respective Job
  val cConfig = ConfigFactory.load("application.conf")
  val mPattern: Pattern = Pattern.compile(cConfig.getString("evValues.mPattern"))                  // Message Regex Pattern
  val lPattern: Pattern = Pattern.compile(cConfig.getString("evValues.lPattern"))                  // Log Regex Pattern
  val outPattern: Pattern = Pattern.compile(cConfig.getString("evValues.MR2.OutPatternType"))      // Intermediate Pattern for Final Reduction
  val windowTime: Int = cConfig.getInt("evValues.MR2.TimeWindow")                                  // This Is Interval Time For Job-2 Default Value is 1 Minute
  val msgType: String = cConfig.getString("evValues.MR2.MessageType")                              // This the Message Type We are Trying to Find
  val resetTime: LocalTime = LocalTime.parse(cConfig.getString("evValues.MR2.resetTime"))          // This is a helper varilable for creating the Intervals


  //Converting exact Time to a Window of Time in Minutes
  def createWindowTime(eTime: String, windowMintues: Int): (String, String)={
    val entryTime: LocalTime = LocalTime.parse(eTime)
    val startTime: LocalTime = resetTime.plusHours(entryTime.getHour()).plusMinutes(entryTime.getMinute)
    val endTime: LocalTime = startTime.plusMinutes(windowMintues)
    (startTime.toString+":00",endTime.toString+":00")
  }

  //  This is an Intermediate Map class
  class InterMap extends MapReduceBase with Mapper[LongWritable, Text, Text, IntWritable]{
    // This method matches the log message to the log pattern, if matches, writes to output with time interval as key and a single integer 1 as value
    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit ={
      logger.info("Inside MR 2: InterMapper")
      val lineMatcher = lPattern.matcher(value.toString)
      if (lineMatcher.matches()){
        val msgMatcher = mPattern.matcher(lineMatcher.group(5))

        if(lineMatcher.group(3) == msgType && msgMatcher.matches()){
          logger.info(s"Inside MR 2: InterMapper: MatchSuccess Msg=${lineMatcher.group(5)}")
          val (startTime, endTime) = createWindowTime(lineMatcher.group(1),windowTime)
          output.collect(new Text(startTime.concat("-").concat(endTime)), new IntWritable(1))
        }
      }
    }
  }

  //  This is an Intermediate Reduce class
  class InterReduce extends MapReduceBase with Reducer[Text, IntWritable, Text, IntWritable]{
    // This method get the input as key value pair, which are the outputs of mapper group together
    // Sums all values of "values" together and outputs the key, sum as pair
    @throws[IOException]
    @throws[InterruptedException]
    override def reduce(key: Text, values: util.Iterator[IntWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit ={
      logger.info("Inside MR 2: InterReducer")
      val sum = values.asScala.reduce((valueOne, valueTwo) => IntWritable(valueOne.get() + valueTwo.get()))
      output.collect(key, new IntWritable(sum.get()))
    }
  }
  // Custom sort key comparator class, used to sort the grouped outputs of map which will be passed to Reducer
  class DesendComparator() extends WritableComparator(classOf[Text], true){
    // This method is used to decide sort criteria for keys that needs to be fed to the reducer
    // @return -1 for if valOne > valTwo, 1 if valOne < valTwo and 0 if both are exactly same
    override def compare(valOne: WritableComparable[_], valTwo: WritableComparable[_]): Int ={
      val firstList = valOne.asInstanceOf[Text].toString.split(",")
      val secondList = valTwo.asInstanceOf[Text].toString.split(",")
      val comparison = firstList(1).toInt.compareTo(secondList(1).toInt)
      if comparison == 0 then
        -1 * firstList(0).compareTo(secondList(0))
      else
        -1 * comparison
    }
  }
 //  Map class for Job 2
  class Map extends MapReduceBase with Mapper[LongWritable, Text, Text, NullWritable] {
    val nullVal: NullWritable = NullWritable.get()

   // This method matches the intermediate output text "(time interval, count) format" to the a reducer input data pattern, NullWritable object is used as value
    @throws[IOException]
    @throws[InterruptedException]
    override def map(key: LongWritable, value: Text, output: OutputCollector[Text, NullWritable], reporter: Reporter): Unit = {
      logger.info("Inside MR 2: MainMapper")
      val matcher = outPattern.matcher(value.toString)

      // if the val matches "timeInterval" format, add a pair for Reducer
      if (matcher.matches()) {
        output.collect(value, nullVal)
      }
    }
  }

  //  Reduce class for Job 2
  class Reduce extends MapReduceBase with Reducer[Text, NullWritable, Text, IntWritable] {
    //  This method get the input as key value pair, which are the outputs of mapper group together
    //  After match it outputs interval as key and error count as value in key-value pair
    @throws[IOException]
    @throws[InterruptedException]
    override def reduce(key: Text, values: util.Iterator[NullWritable], output: OutputCollector[Text, IntWritable], reporter: Reporter): Unit = {
      logger.info("Inside MR 2: MainReducer")
      val matcher = outPattern.matcher(key.toString)

      // if the key matches "timeInterval" format, add a pair for Result
      if (matcher.matches()) {
        output.collect(new Text(matcher.group(1)), new IntWritable(matcher.group(2).toInt))
      }
    }
  }

}



