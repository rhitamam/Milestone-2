package test.distributed

import breeze.linalg._
import breeze.numerics._
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfterAll
import shared.predictions._
import test.shared.helpers._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext

class ExactTests extends AnyFunSuite with BeforeAndAfterAll {
  
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
   test("kNN predictor with k=10") { 

     val avg = avgRating(train2)
     val avgMatrix = avgRatingUserMatrix(train2,943, 1682)
     val normalizedMatrix = normalizedDevMatrix(train2,avgMatrix,943, 1682)
     val preProcessedMatrix = processedMatrix(normalizedMatrix,943, 1682)
     val pred = parallelizedKNN(train2,10,sc,943, 1682)

     // Similarity between user 1 and itself
     assert(within(simkNNparallelized(1,1,10,preProcessedMatrix,1682), 0.0, 0.0001))
 
     // Similarity between user 1 and 864
     assert(within(simkNNparallelized(1,864,10,preProcessedMatrix,1682), 0.24232304952129655, 0.0001))

     // Similarity between user 1 and 886
     assert(within(simkNNparallelized(1,886,10,preProcessedMatrix,1682), 0.0, 0.0001))

     // Prediction user 1 and item 1
     assert(within(pred(1,1), 4.319093503763853, 0.0001))

     // Prediction user 327 and item 2
     assert(within(pred(327,2), 2.6994178006921192, 0.0001))

     // MAE on test
     assert(within(maeCSC(pred,test2), 0.8287277961963542, 0.0001)) 
   } 
}
