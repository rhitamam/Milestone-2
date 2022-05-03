package shared

import breeze.linalg._
import breeze.numerics._
import scala.io.Source
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.SparkContext

package object predictions
{
  // ------------------------ For template
  case class Rating(user: Int, item: Int, rating: Double)

  def timingInMs(f : ()=>Double ) : (Double, Double) = {
    val start = System.nanoTime() 
    val output = f()
    val end = System.nanoTime()
    return (output, (end-start)/1000000.0)
  }

  def toInt(s: String): Option[Int] = {
    try {
      Some(s.toInt)
    } catch {
      case e: Exception => None
    }
  }

  def mean(s :Seq[Double]): Double =  if (s.size > 0) s.reduce(_+_) / s.length else 0.0

  def std(s :Seq[Double]): Double = {
    if (s.size == 0) 0.0
    else { 
      val m = mean(s)
      scala.math.sqrt(s.map(x => scala.math.pow(m-x, 2)).sum / s.length.toDouble) 
    }
  }


  def load(path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
    val file = Source.fromFile(path)
    val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies) 
    for (line <- file.getLines) {
      val cols = line.split(sep).map(_.trim)
      toInt(cols(0)) match {
        case Some(_) => builder.add(cols(0).toInt-1, cols(1).toInt-1, cols(2).toDouble)
        case None => None
      }
    }
    file.close
    builder.result()
  }

  def loadSpark(sc : org.apache.spark.SparkContext,  path : String, sep : String, nbUsers : Int, nbMovies : Int) : CSCMatrix[Double] = {
    val file = sc.textFile(path)
    val ratings = file
      .map(l => {
        val cols = l.split(sep).map(_.trim)
        toInt(cols(0)) match {
          case Some(_) => Some(((cols(0).toInt-1, cols(1).toInt-1), cols(2).toDouble))
          case None => None
        }
      })
      .filter({ case Some(_) => true
                 case None => false })
      .map({ case Some(x) => x
             case None => ((-1, -1), -1) }).collect()

    val builder = new CSCMatrix.Builder[Double](rows=nbUsers, cols=nbMovies)
    for ((k,v) <- ratings) {
      v match {
        case d: Double => {
          val u = k._1
          val i = k._2
          builder.add(u, i, d)
        }
      }
    }
    return builder.result
  }

  def partitionUsers (nbUsers : Int, nbPartitions : Int, replication : Int) : Seq[Set[Int]] = {
    val r = new scala.util.Random(1337)
    val bins : Map[Int, collection.mutable.ListBuffer[Int]] = (0 to (nbPartitions-1))
       .map(p => (p -> collection.mutable.ListBuffer[Int]())).toMap
    (0 to (nbUsers-1)).foreach(u => {
      val assignedBins = r.shuffle(0 to (nbPartitions-1)).take(replication)
      for (b <- assignedBins) {
        bins(b) += u
      }
    })
    bins.values.toSeq.map(_.toSet)
  }

  // Useful functions

  def maeCSC(predictor : (Int,Int) => Double, test : CSCMatrix[Double]) : Double = {
    var s = 0.0
    for ((k,v) <- test.activeIterator) {
      val u = k._1
      val i = k._2
      s = s + (predictor(u,i) - v).abs
    }
    s/test.activeSize
  }

  // Part 3: Optimizing with Breeze, a Linear Algebra Library

  def vectorOnes(n : Int) : CSCMatrix[Double] = {
    val builder = new CSCMatrix.Builder[Double](rows=n, cols=1)
    for (k <- 0 to n-1) {
      builder.add(k,0,1)
    }
    builder.result()
  }

  def avgRating(ratings : CSCMatrix[Double]) : Double = {
    sum(ratings)/ratings.activeSize
  }

  def avgRatingUser(user : Int, ratings : CSCMatrix[Double]) : Double = {
    val mat = ratings(user, 0 to (ratings.cols - 1)).t
    sum(mat)/mat.findAll(_ > 0).size
  }

  def users(ratings : CSCMatrix[Double]) : Seq[Int] = {
    var users: Seq[Int] = Seq.empty
    for ((k,v) <- ratings.activeIterator) {
      val u = k._1
      users = users :+ u
    }
    users.distinct
  }

  def avgRatingUserMatrix(ratings : CSCMatrix[Double], userList : Seq[Int]) : CSCMatrix[Double] = {
    val builder = new CSCMatrix.Builder[Double](rows=ratings.rows, cols=1)
    for (k <- userList) {
      builder.add(k, 0, avgRatingUser(k, ratings))
    }
    builder.result()
  }

  def scale(x : Double, y : Double) : Double = 
    if (x > y) 5-y else if (x < y) y-1 else 1

  def normalizedDev(r_u_i : Double, user : Int, avgMatrix : CSCMatrix[Double]) : Double = {
    val r_u = avgMatrix(user,0)
    (r_u_i - r_u)/scale(r_u_i, r_u)
  }

  def normalizedDevMatrix(ratings : CSCMatrix[Double], avgMatrix : CSCMatrix[Double]) : CSCMatrix[Double] = {
    val builder = new CSCMatrix.Builder[Double](rows=ratings.rows, cols=ratings.cols)
    for ((k,v) <- ratings.activeIterator) {
      val u = k._1
      val i = k._2
      builder.add(u, i, normalizedDev(v,u,avgMatrix))
    }
    builder.result()
  }

  def processedMatrix(normalizedMatrix : CSCMatrix[Double], userList : Seq[Int]) : CSCMatrix[Double] = {
    var sumSquareMatrix = (normalizedMatrix *:* normalizedMatrix) * vectorOnes(normalizedMatrix.cols)
    for ((k,v) <- sumSquareMatrix.activeIterator) {
      val row = k._1
      val col = k._2
      sumSquareMatrix(row,col) = scala.math.sqrt(v)
    }
    var mat = normalizedMatrix.copy
    for ((k,v) <- normalizedMatrix.activeIterator) {
      val row = k._1
      val col = k._2
      mat(row,col) = v/sumSquareMatrix(row,0)
    }
    mat
  }

  def sim(k : Int, preProcessedMatrix : CSCMatrix[Double], userList : Seq[Int]) : CSCMatrix[Double] = {
    var simMat = preProcessedMatrix * preProcessedMatrix.t
    val builder = new CSCMatrix.Builder[Double](rows=userList.length, cols=userList.length)
    for (u <- userList) {
      for (v <- argtopk(simMat(u,0 to simMat.cols - 1).t,k+1)) {
        if (u != v) builder.add(u, v, simMat(u,v))
      }
    }
    builder.result()
  }

  def kNNRating(user : Int, item : Int, normalizedMatrix : CSCMatrix[Double], simMatrix : CSCMatrix[Double],userList : Seq[Int], ratings : CSCMatrix[Double]) : Double = {
    var num = 0.0
    var denom = 0.0
    for (v <- userList) {
      if (ratings(v,item) != 0) {
        num = num + simMatrix(user,v)*normalizedMatrix(v,item)
        denom = denom + simMatrix(user,v).abs
      }
    }
    if (denom != 0) num/denom else 0.0
  }

  def simkNN(u : Int, v : Int, k : Int, ratings : CSCMatrix[Double]) : Double = {
    val userList = users(ratings)
    val avgMatrix = avgRatingUserMatrix(ratings,userList)
    val normalizedMatrix = normalizedDevMatrix(ratings,avgMatrix)
    val preProcessedMatrix = processedMatrix(normalizedMatrix,userList)
    val simMatrix = sim(k,preProcessedMatrix,userList)
    simMatrix(u,v)
  }

  def predictionKNN(k : Int, ratings : CSCMatrix[Double]) : ((Int,Int) => Double) = {
    val avg = avgRating(ratings)
    val userList = users(ratings)
    val avgMatrix = avgRatingUserMatrix(ratings,userList)
    val normalizedMatrix = normalizedDevMatrix(ratings,avgMatrix)
    val preProcessedMatrix = processedMatrix(normalizedMatrix,userList)
    val simMatrix = sim(k,preProcessedMatrix,userList)
    var r_u : Double = 0.0
    ((u,i) => {
    var r_i = kNNRating(u,i,normalizedMatrix,simMatrix,userList,ratings)
    if (userList contains u) {r_u = avgMatrix(u,0)} else avg
    r_u + r_i * scale(r_u + r_i, r_u)
    })
  }

  // Part 4: Parallel k-NN Computations with Replicated Ratings

  // Part 5: Distributed Approximate k-NN

  // Part 6: Economics

}


