import com.typesafe.config.ConfigFactory
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers

class MRSpecTest extends AnyFlatSpec with Matchers {

  "The application.conf" should "contain the correct values" in {
    val config = ConfigFactory.load("application.conf")

    config.getString("evValues.mPattern") should be("(.*?)")
    config.getString("evValues.lPattern") should be("(.+)\\s\\[(.+)\\]\\s+(WARN|ERROR|DEBUG|INFO)\\s+(.+)\\s+-\\s+(.+)\\s*")

    val mr1 = config.getConfig("evValues.MR1")
    mr1.getInt("MapperCount") should be(4)
    mr1.getInt("ReducerCount") should be(1)
    mr1.getString("MRId") should be("MR1")
    mr1.getString("TaskJobName") should be("MRJob1")

  }
}
