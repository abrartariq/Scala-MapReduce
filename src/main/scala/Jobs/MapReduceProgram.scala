package Jobs

import org.slf4j.{Logger, LoggerFactory}
import com.typesafe.config.{Config, ConfigFactory}
import org.apache.hadoop.fs.{LocalFileSystem, Path}
import org.apache.hadoop.io.{IntWritable, LongWritable, NullWritable, Text}
import org.apache.hadoop.mapred.*

import java.io.{File, IOException}
import java.nio.file.{Files, Paths, StandardCopyOption}
import java.time.LocalTime
import java.util
import scala.reflect.io.Directory
import scala.util.matching.Regex
import HelperUtils.CreateLogger
import org.apache.hadoop.hdfs.DistributedFileSystem
import org.eclipse.jetty.client.api.Destination


object MapReduceProgram {

  //  Logger For MapReduceProgram
  val logger: Logger = CreateLogger(getClass)

  //  Config Var
  val cConfig: Config = ConfigFactory.load("application.conf")

  //  Helper Function For clearing the Directory
  def deleteRecursively(file: File): Unit = {
    if (file.isDirectory) {
      file.listFiles.foreach(deleteRecursively)
    }
    if (file.exists && !file.delete) {
      throw new Exception(s"Unable to delete ${file.getAbsolutePath}")
    }
  }
  // Renaming Files to CSV After Output
  def moveRenameFile(source: String): Unit = {
    if (source.takeRight(1) == "/"){
      renamingHelper(source + cConfig.getString("evValues.RenameHelper.Source"), source + cConfig.getString("evValues.RenameHelper.Destination"))
    }else{
      renamingHelper(source + "/" + cConfig.getString("evValues.RenameHelper.Source"), source + "/" + cConfig.getString("evValues.RenameHelper.Destination"))
    }
  }
  // Renaming Files Helper
  def renamingHelper(source: String, destination: String): Unit ={
    val path = Files.move(
      Paths.get(source),
      Paths.get(destination),
      StandardCopyOption.REPLACE_EXISTING
    )
  }
  //  Clearing All OutPut Directory for Current Run
  def clearOutputDir(dirPath: String): Unit = {
    val dir = new Directory(new File(dirPath))
    dir.deleteRecursively()
  }
  //  Map-reduce Job 1
  def executeMRJob1(inputPath: String, outputPath: String): Unit = {
    clearOutputDir(outputPath)
    val conf = new JobConf(classOf[MRJob1.type])
    conf.setJobName(cConfig.getString("evValues.MR1.TaskJobName"))
    conf.set("mapreduce.job.maps", cConfig.getString("evValues.MR1.MapperCount"))
    conf.set("mapreduce.job.reduces", cConfig.getString("evValues.MR1.ReducerCount"))
    conf.set("mapreduce.output.textoutputformat.separator",",")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[MRJob1.Map])
    conf.setCombinerClass(classOf[MRJob1.Reduce])
    conf.setReducerClass(classOf[MRJob1.Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
//    moveRenameFile(outputPath)
  }


  //  Map-reduce Job 2 Starting point
  def executeInterMRJob2(inputPath: String, intermediateOutputPath: String, outputPath: String): Unit ={
    clearOutputDir(outputPath)
    clearOutputDir(intermediateOutputPath)
    val conf = new JobConf(classOf[MRJob2.type])
    conf.setJobName(cConfig.getString("evValues.MR2.InterMRJobName"))
    conf.set("mapreduce.job.maps", cConfig.getString("evValues.MR2.InterMapperCount"))
    conf.set("mapreduce.job.reduces", cConfig.getString("evValues.MR2.InterReducerCount"))
    conf.set("mapreduce.output.textoutputformat.separator",",")
    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[MRJob2.InterMap])
    conf.setReducerClass(classOf[MRJob2.InterReduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(intermediateOutputPath))
    val runningJob = JobClient.runJob(conf)

    // Waiting for intermediate job to complete
    runningJob.waitForCompletion()
    if (runningJob.isSuccessful) {
      logger.info("the intermediate job finished successfully, starting the final job")
      this.executeMRJob2(intermediateOutputPath, outputPath)
    } else {
      logger.error("The intermediate job failed, unable to start the final job")
    }
  }

  //  Map-reduce Job 2 Concluding Step
  def executeMRJob2(inputPath: String, outputPath: String): Unit = {
    val conf = new JobConf(classOf[MRJob2.type])
    conf.setJobName(cConfig.getString("evValues.MR2.TaskJobName"))
    conf.set("mapreduce.job.maps", cConfig.getString("evValues.MR2.MapperCount"))
    conf.set("mapreduce.job.reduces", cConfig.getString("evValues.MR2.ReducerCount"))
    conf.set("mapreduce.output.textoutputformat.separator",",")
    conf.set("fs.hdfs.impl", classOf[DistributedFileSystem].getName)
    conf.set("fs.file.impl", classOf[LocalFileSystem].getName)
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[MRJob2.Map])
    conf.setMapOutputKeyClass(classOf[Text])
    conf.setMapOutputValueClass(classOf[NullWritable])
    conf.setReducerClass(classOf[MRJob2.Reduce])
    conf.setOutputKeyComparatorClass(classOf[MRJob2.DesendComparator])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
//    moveRenameFile(outputPath)

  }

  //  Map-reduce Job 3
  def executeMRJob3(inputPath: String, outputPath: String): Unit = {
    clearOutputDir(outputPath)
    val conf = new JobConf(classOf[MRJob3.type])
    conf.setJobName(cConfig.getString("evValues.MR3.TaskJobName"))
    conf.set("mapreduce.job.maps", cConfig.getString("evValues.MR3.MapperCount"))
    conf.set("mapreduce.job.reduces", cConfig.getString("evValues.MR3.ReducerCount"))
    conf.set("mapreduce.output.textoutputformat.separator",",")
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[MRJob3.Map])
    conf.setCombinerClass(classOf[MRJob3.Reduce])
    conf.setReducerClass(classOf[MRJob3.Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
//    moveRenameFile(outputPath)

  }

  //  Map-reduce Job 4
  def executeMRJob4(inputPath: String, outputPath: String): Unit = {
    clearOutputDir(outputPath)
    val conf = new JobConf(classOf[MRJob4.type])
    conf.setJobName(cConfig.getString("evValues.MR4.TaskJobName"))
    conf.set("mapreduce.job.maps", cConfig.getString("evValues.MR4.MapperCount"))
    conf.set("mapreduce.job.reduces", cConfig.getString("evValues.MR4.ReducerCount"))
    conf.set("mapreduce.output.textoutputformat.separator",",")
    conf.setMapOutputKeyClass(classOf[Text])
    conf.setMapOutputValueClass(classOf[Text])
    conf.setOutputKeyClass(classOf[Text])
    conf.setOutputValueClass(classOf[IntWritable])
    conf.setMapperClass(classOf[MRJob4.Map])
    conf.setReducerClass(classOf[MRJob4.Reduce])
    conf.setInputFormat(classOf[TextInputFormat])
    conf.setOutputFormat(classOf[TextOutputFormat[Text, IntWritable]])
    FileInputFormat.setInputPaths(conf, new Path(inputPath))
    FileOutputFormat.setOutputPath(conf, new Path(outputPath))
    JobClient.runJob(conf)
//    moveRenameFile(outputPath)
}

  // Main method - entry point of application
  // This method decides which task to execute based on parameters passed
  @main def runMapReduce(jobType: String,inputPath: String, outputPath: String) = {

    logger.info("Logging Begins")
    val MR1ID: String = cConfig.getString("evValues.MR1.MRId")
    val MR2ID: String = cConfig.getString("evValues.MR2.MRId")
    val MR3ID: String = cConfig.getString("evValues.MR3.MRId")
    val MR4ID: String = cConfig.getString("evValues.MR4.MRId")
    val Temp: String = cConfig.getString("evValues.RenameHelper.Temp")
    
    // Matching Job ID to Job Type (MR1, MR2, MR3, MR4)
    jobType match{
      case "All" => {
        executeMRJob1(inputPath, outputPath+MR1ID)
        executeInterMRJob2(inputPath, outputPath+Temp+MR2ID,outputPath+MR2ID)
        executeMRJob3(inputPath, outputPath+MR3ID)
        executeMRJob4(inputPath, outputPath+MR4ID)
      }
      case MR1ID => executeMRJob1(inputPath, outputPath+MR1ID)
      case MR2ID => executeInterMRJob2(inputPath, outputPath+Temp+MR2ID,outputPath+MR2ID)
      case MR3ID => executeMRJob3(inputPath, outputPath+MR3ID)
      case MR4ID => executeMRJob4(inputPath, outputPath+MR4ID)
      case _ => 1+1
    }
    logger.info("Logging Ends")
  }
}
