package test.distributed

import breeze.linalg._
import breeze.numerics._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import shared.predictions._
import test.shared.helpers._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

class ApproximateTests extends AnyFunSuite with BeforeAndAfterAll {
  
   val separator = "\t"
   val train2Path = "data/ml-100k/u2.base"
   val test2Path = "data/ml-100k/u2.test"
   var train2 : CSCMatrix[Double] = null
   var test2 : CSCMatrix[Double] = null
   var sc : SparkContext = null

   override def beforeAll {
     train2 = load(train2Path, separator, 943, 1682)
     test2 = load(test2Path, separator, 943, 1682)

     val spark = SparkSession.builder().master("local[2]").getOrCreate();
     spark.sparkContext.setLogLevel("ERROR")
     sc = spark.sparkContext
   }

   // Provide tests to show how to call your code to do the following tasks.
   // Ensure you use the same function calls to produce the JSON outputs in
   // the corresponding application.
   // Add assertions with the answer you expect from your code, up to the 4th
   // decimal after the (floating) point, on data/ml-100k/u2.base (as loaded above).
   test("Approximate kNN predictor with 10 partitions and replication of 2") { 
    var partitionedUsers : Seq[Set[Int]] = partitionUsers(
      943, 
      10, 
      2 
    )

     // Similarity between user 1 and itself
     assert(within(1.0, 0.0, 0.0001))
 
     // Similarity between user 1 and 864
     assert(within(1.0, 0.0, 0.0001))

     // Similarity between user 1 and 344
     assert(within(1.0, 0.0, 0.0001))

     // Similarity between user 1 and 16
     assert(within(1.0, 0.0, 0.0001))

     // Similarity between user 1 and 334
     assert(within(1.0, 0.0, 0.0001))

     // Similarity between user 1 and 2
     assert(within(1.0, 0.0, 0.0001))

     // MAE on test
     assert(within(1.0, 0.0, 0.0001))
   } 
}
