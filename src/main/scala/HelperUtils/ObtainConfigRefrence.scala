package HelperUtils

import com.typesafe.config.{Config, ConfigFactory}

import scala.util.{Failure, Success, Try}


// Loads the config file, validates for existence and then returns the config
object ObtainConfigReference {
  private val config = ConfigFactory.load()
  private val logger = CreateLogger(classOf[ObtainConfigReference.type])

  private def ValidateConfig(confEntry: String): Boolean = Try(config.getConfig(confEntry)) match {
    case Failure(exception) => logger.error(s"Failure in Reading $confEntry error : $exception"); false
    case Success(_) => true
  }
  def apply(confEntry: String): Option[Config] = {
    if (ValidateConfig(confEntry)){
      Some(config)
    }else{
      None
    }
  }
}

