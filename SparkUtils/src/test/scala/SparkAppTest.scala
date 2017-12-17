import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.{FeatureSpec, GivenWhenThen, Matchers}
import com.jrgv89.spark.utils.dataframe.Implicits._
class SparkAppTest extends FeatureSpec with GivenWhenThen with Matchers {

  val spark: SparkSession = SparkSession.builder().master("local").getOrCreate()

  spark.get(Seq(""))


  feature("spark app") {

    scenario("Process") {

      Given("Given a environment with data and a dataframe to test")


      When("When execute process")


      Then("Then should get array of ParametricTableRow equals to test")


    }
  }
}
