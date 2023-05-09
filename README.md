Map_Reduce

Name = **Muhammad Abrar Tariq**

### Link Demonstrating deployment **[Video](https://drive.google.com/drive/folders/1QsBj7ddcb8OfHoBfcEoSnL646vOoiVy-?usp=sharing)**

### Steps to Set-up Environment.

* Cloning the Project -
 
```git clone https://github.com/abrartariq/Scala-MapReduce.git```



      *Install IntelliJ or your preferred IDE. This application was developed using IntelliJ, which is highly recommended for various reasons.
      *Ensure that you have at least Java SDK version 8 installed on your machine.
      *Verify that Git (version control) is installed on your machine. If not, please install it.
      *Open IntelliJ and import the project into the IDE environment. Alternatively, choose New -> Project from Version Control, and enter the GitHub URL of this repository to load it directly into your system if not already cloned.
      *The application is written in Scala programming language, using version 3.1.3.
      *This Requires Hadoop to be Installed on the machine (Look at Refrence for detail)
      *To build the project, I used sbt (Scala's Simple Build Tool), specifically version 1.7.2.
      *You can find all dependencies and build settings in the build.sbt file.
      *Once IntelliJ has successfully detected the appropriate Scala and sbt versions, the project is ready to be compiled/built.
      *Open the terminal at the bottom of IntelliJ or use any terminal with the path set to the project's root directory.
      *Enter sbt clean compile to start building the project.
      *Test cases are written using the ScalaTest library.
      
      
#### Intro
This project involves demonstration of hadoop framework to perform map reduce tasks using distributive computing. 
All the tasks that belong to this homework are focused on extracting meaningful data from log messages. These log messages 
are generated using a Random message generator script. The whole idea of this project is to read tons of data full of log messages, 
filter the log messages that match a particular pattern and then give various insight into this data by outputting a concise CSV data format output.
More details about implementation, data semantics and deployment are given in later sections of this documentation.

#### Job 1 : Time Constraint Distribution of Messages.

This task's input will be start time and end time in format "HH:mm:ss.SSS".
If any log message having time within this range will be matched with string pattern.
The result of this task would be show how many log messages of each type (Error, Warn, Info, Debug) are matched that belong to that time range or period.

The input start time and end time is provided from "application.conf" file.

### [MRJob1](src/main/scala/Jobs/MRJob1.scala)
| Name | Run CMD |
| :---: | :---: |
| [sbt-run](src/main/scala/Jobs/MRJob1.scala) | sbt "run MR1 src/main/resources/input src/main/resources/output" |
| [Hadoop Run](src/main/scala/Jobs/MRJob1.scala) | hadoop /jar-file-location MR1 /user/input/ /user/output/ |

```
DEBUG,1379
ERROR,81
INFO,9529
WARN,2524
```
This means there are 1379 log message with debug level whose string message matched a particular pattern. 
The same goes for other levels as well.

#### Job 2 : Interval Grouping with Max Count.
This task's input will be a time interval in seconds (provided from config file). 
This task calculates the number of "ERROR" level log message for each fixed time interval in log messages in descending order.

### [MRJob2](src/main/scala/Jobs/MRJob2.scala)
| Name | Run CMD |
| :---: | :---: |
| [sbt-run](src/main/scala/Jobs/MRJob2.scala) | sbt "run MR2 src/main/resources/input src/main/resources/output" |
| [Hadoop Run](src/main/scala/Jobs/MRJob2.scala) | hadoop /jar-file-location MR2 /user/input/ /user/output/ |

In this task, an intermediate output folder is created (used). This is because, the task 2 implemented using two jobs, first job
outputs the result in the intermediate output folder. Second job will take the result of first job from intermediate output folder as input and outputs the final result in the output folder.

```
02:24:00-02:25:00,38
02:25:00-02:26:00,25
02:04:00-02:05:00,19
02:06:00-02:07:00,27
```
The above is calculated for time interval 1 minute by specifying it in config file.
The first entry says that between 02:24:00 and 02:25:00 timestamps, there are 38 error messages found.
It can be seen that it is in descending order of number of error messages.

#### Job 3 : Total Count of Messages.

This Task is spliting image of Task 1 where rather that filtering record based on interval we Look at full log.

### [MRJob3](src/main/scala/Jobs/MRJob3.scala)
| Name | Run CMD |
| :---: | :---: |
| [sbt-run](src/main/scala/Jobs/MRJob3.scala) | sbt "run MR3 src/main/resources/input src/main/resources/output" |
| [Hadoop Run](src/main/scala/Jobs/MRJob3.scala) | hadoop /jar-file-location MR3 /user/input/ /user/output/ |

```
DEBUG,1854
ERROR,108
INFO,12690
WARN,3402
```
This sample output represents that total of 1854 debug level messages are found in all input logs. The same goes for other log levels.

#### Job  4: Messages Group by Highest Number of Characters.

This task computes the length of log message which has maximum character length of the matched string message with the string pattern for each log type.

### [MRJob4](src/main/scala/Jobs/MRJob4.scala)
| Name | Run CMD |
| :---: | :---: |
| [sbt-run](src/main/scala/Jobs/MRJob4.scala) | sbt "run MR4 src/main/resources/input src/main/resources/output" |
| [Hadoop Run](src/main/scala/Jobs/MRJob4.scala) | hadoop /jar-file-location MR4 /user/input/ /user/output/ |


```
DEBUG,128
ERROR,142
INFO,86
WARN,134
```
This means that for DEBUG level, the log message with the largest character length of the string instance has a length og 128 characters.
It is to be Noted that the Char Length is for Whole Log Line not just the Message.


### [MapReduceProgram](src/main/scala/Jobs/MapReduceProgram.scala)
| Name | Run CMD |
| :---: | :---: |
| [sbt-run](src/main/scala/Jobs/MapReduceProgram.scala) | sbt "run MR4 src/main/resources/input src/main/resources/output" |
| [Hadoop Run](src/main/scala/Jobs/MapReduceProgram.scala) | hadoop /jar-file-location MR4 /user/input/ /user/output/ |



```
  @main def runMapReduce(jobType: String,inputPath: String, outputPath: String) = {

    logger.info("Logging Begins")
    val MR1ID: String = cConfig.getString("evValues.MR1.MRId")
    val MR2ID: String = cConfig.getString("evValues.MR2.MRId")
    val MR3ID: String = cConfig.getString("evValues.MR3.MRId")
    val MR4ID: String = cConfig.getString("evValues.MR4.MRId")
    val Temp: String = cConfig.getString("evValues.RenameHelper.Destination")

    // Matching Job ID to Job Type (All, MR1, MR2, MR3, MR4)
    jobType match{
      case "All" => {
        executeMRJob1(inputPath, outputPath+MR1ID)
        executeInterMRJob2(inputPath, outputPath+Temp+MR2ID,outputPath+MR2ID)
        executeMRJob3(inputPath, outputPath+MR3ID)
        executeMRJob4(inputPath, outputPath+MR4ID)
      }
      case MR1ID => executeMRJob1(inputPath, outputPath+MR1ID)
      case MR2ID => executeInterMRJob2(inputPath, outputPath+"Temp"+MR2ID,outputPath+MR2ID)
      case MR3ID => executeMRJob3(inputPath, outputPath+MR3ID)
      case MR4ID => executeMRJob4(inputPath, outputPath+MR4ID)
      case _ => 1+1
    }
    logger.info("Logging Ends")
  }
```

This code defines a main function called runMapReduce to run MapReduce jobs.


To use the runMapReduce function, call it with three parameters:

    jobType: a String that specifies which MapReduce job to run. It can be one of the following: "All", "MR1", "MR2", "MR3", or "MR4".
    inputPath: a String that specifies the input path for the MapReduce job.
    outputPath: a String that specifies the output path for the MapReduce job.

Here's an example of how to call the function:

scala
```
sbt "run All /input/path/ /output/path/
```

Functionality

The runMapReduce function initializes some variables by reading values from a configuration file called cConfig. It then uses pattern matching on the jobType parameter to determine which MapReduce job to run.

If the jobType is "All", it runs all the four MapReduce jobs one after the other. If the jobType is any of the four individual MapReduce job IDs (MR1ID, MR2ID, MR3ID, MR4ID), then it runs only the corresponding MapReduce job. If the jobType is none of the above, then it returns 2 as an error code.

It logs information at the beginning and end of the function using a logger.

### AWS EMR
The deployment of this application to AWS EMR is done in detail Step shown in the video **[Video-Link](https://drive.google.com/drive/folders/1QsBj7ddcb8OfHoBfcEoSnL646vOoiVy-?usp=sharing)**.
The documentation for AWS EMR can be found [here](https://docs.aws.amazon.com/emr/index.html).



## References
1. [Scala](https://docs.scala-lang.org/)
2. [MapReduce](https://hadoop.apache.org/docs/stable/hadoop-project-dist/hadoop-common/SingleCluster.html)
3. [AWS EMR](https://aws.amazon.com/emr/)

