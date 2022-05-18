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
      s = s + (predictor(u+1,i+1) - v).abs
    }
    s/test.activeSize
  }

  def simkNN(u : Int, v : Int, simMatrix : CSCMatrix[Double]) : Double = {
    simMatrix(u-1,v-1)
  }

  def prodMat(mat : CSCMatrix[Double], row : Int, col : Int) : CSCMatrix[Double] = {
    val builder = new CSCMatrix.Builder[Double](rows=row, cols=row)
    for (v <- 0 to row-1){ 
      val column_v = mat * mat(v,0 to col-1).t
      for (u <- 0 to row-1) {
        builder.add(u,v,column_v(u))
      }
    }
    builder.result()
  }

  def prodMat2(mat : CSCMatrix[Double], row : Int, col : Int) : CSCMatrix[Double] = {
    val builder = new CSCMatrix.Builder[Double](rows=row, cols=row)
    for (u <- 0 to row -1){
      for (v <- 0 to row-1){ 
        builder.add(u,v,mat(u,0 to col-1) * mat(v,0 to col-1).t)
      }
    }
    builder.result()
  }

  def reduction(mat : CSCMatrix[Double],users : Int) : DenseVector[Double] = {
    var vect = DenseVector.zeros[Double](users)	
    for ((k,v) <- mat.activeIterator) {
      val row = k._1
      val col = k._2
      vect(row) = vect(row) + mat(row,col)
    }
    vect
  }

  def vectorOnes(n : Int) : CSCMatrix[Double] = {
    val builder = new CSCMatrix.Builder[Double](rows=n, cols=1)
    for (k <- 0 to n-1) {
      builder.add(k,0,1)
    }
    builder.result()
  }

  // Part 3: Optimizing with Breeze, a Linear Algebra Library

  def avgRating(ratings : CSCMatrix[Double]) : Double = {
    sum(ratings)/ratings.activeSize
  }

  def avgRatingUserMatrix(ratings : CSCMatrix[Double], users : Int, movies : Int) : CSCMatrix[Double] = {
    val counts = Array.fill(users)(0)
    val builder = new CSCMatrix.Builder[Double](rows=users, cols=1)
    for ((k,v) <- ratings.activeIterator) {
      val u = k._1
      builder.add(u, 0, v)
      counts(u) = counts(u) + 1
    }
    val avgMat = builder.result()
    for (u <- 0 to users-1){
      if (counts(u) != 0) {avgMat(u,0) = avgMat(u,0)/counts(u)} else {avgMat(u,0) = 0.0}
    }
    avgMat
  }

  def scale(x : Double, y : Double) : Double = 
    if (x > y) 5-y else if (x < y) y-1 else 1

  def normalizedDev(r_u_i : Double, user : Int, avgMatrix : CSCMatrix[Double]) : Double = {
    val r_u = avgMatrix(user,0)
    (r_u_i - r_u)/scale(r_u_i, r_u)
  }

  def normalizedDevMatrix(ratings : CSCMatrix[Double], avgMatrix : CSCMatrix[Double], users : Int, movies : Int) : CSCMatrix[Double] = {
    val builder = new CSCMatrix.Builder[Double](rows=users, cols=movies)
    for ((k,v) <- ratings.activeIterator) {
      val u = k._1
      val i = k._2
      builder.add(u, i, normalizedDev(v,u,avgMatrix))
    }
    builder.result()
  }

  def processedMatrix(normalizedMatrix : CSCMatrix[Double], users : Int, movies : Int) : CSCMatrix[Double] = {
    var sumSquareMatrix = (normalizedMatrix *:* normalizedMatrix) * vectorOnes(movies) //if we use the function vectorOnes
    //var sumSquareMatrix = reduction(normalizedMatrix *:* normalizedMatrix,users) //if we use the function reduction
    val builder = new CSCMatrix.Builder[Double](rows=users, cols=movies)
    for ((k,v) <- normalizedMatrix.activeIterator) {
      val row = k._1
      val col = k._2
      builder.add(row,col,v/scala.math.sqrt(sumSquareMatrix(row,0))) //if we use the function vectorOnes
      //builder.add(row,col,v/scala.math.sqrt(sumSquareMatrix(row))) //if we use the function reduction
    }
    builder.result()
  }

  def simOpt(k : Int, preProcessedMatrix : CSCMatrix[Double], users : Int, movies : Int) : CSCMatrix[Double] = {
    var simMat = prodMat(preProcessedMatrix,users,movies)
    val builder = new CSCMatrix.Builder[Double](rows=users, cols=users)
    for (u <- 0 to users-1) {
      for (v <- argtopk(simMat(u,0 to users-1).t,k+1)) {
        if (u != v) builder.add(u, v, simMat(u,v))
      }
    }
    builder.result()
  }

  def kNNRating(user : Int, item : Int, normalizedMatrix : CSCMatrix[Double], simMatrix : CSCMatrix[Double],users : Int, ratings : CSCMatrix[Double]) : Double = {
    var num = 0.0
    var denom = 0.0
    for (v <- 0 to users-1) {
      if (ratings(v,item) != 0) {
        num = num + simMatrix(user,v)*normalizedMatrix(v,item)
        denom = denom + simMatrix(user,v).abs
      }
    }
    if (denom != 0) num/denom else 0.0
  }

  def predictionKNN(k : Int, ratings : CSCMatrix[Double], users : Int, movies : Int) : ((Int,Int) => Double) = {
    val avg = avgRating(ratings)
    val avgMatrix = avgRatingUserMatrix(ratings,users,movies)
    val normalizedMatrix = normalizedDevMatrix(ratings,avgMatrix,users,movies)
    val preProcessedMatrix = processedMatrix(normalizedMatrix,users,movies)
    val simMatrix = simOpt(k,preProcessedMatrix,users,movies)
    var r_u : Double = 0.0
    ((u,i) => {
    var r_i = kNNRating(u-1,i-1,normalizedMatrix,simMatrix,users,ratings)
    if (u-1 >= 0 && u-1 < users) {r_u = avgMatrix(u-1,0)} else avg
    r_u + r_i * scale(r_u + r_i, r_u)
    })
  }

  // Part 4: Parallel k-NN Computations with Replicated Ratings

  def topk(u : Int, br : org.apache.spark.broadcast.Broadcast[CSCMatrix[Double]], k : Int, movies : Int) : (Int,Seq[(Int,Double)]) = {
    val r = br.value
    val sim_u = r * r(u,0 to movies-1).t
    (u,argtopk(sim_u,k+1).filter(_ != u).map(x => (x, sim_u(x))))
  }

  def simkNNparallelized(u : Int, v : Int, k : Int, preProcessedMatrix : CSCMatrix[Double], movies : Int) : Double = {
    val sim_u = preProcessedMatrix * preProcessedMatrix(u-1,0 to movies-1).t
    if (argtopk(sim_u,k+1).filter(_ != u-1) contains v-1) {
      sim_u(v-1)
    } else 0
  }

  def parallelizedKNN(ratings : CSCMatrix[Double], k: Int, sc : SparkContext, users : Int, movies : Int): ((Int,Int) => Double) = {
    val avg = avgRating(ratings)
    val avgMatrix = avgRatingUserMatrix(ratings,users,movies)
    val normalizedMatrix = normalizedDevMatrix(ratings,avgMatrix,users,movies)
    val preProcessedMatrix = processedMatrix(normalizedMatrix,users,movies)
    val br = sc.broadcast(preProcessedMatrix)
    val topku = sc.parallelize(0 to users-1).map(u=> topk(u,br,k,movies)).collect()
    val builder = new CSCMatrix.Builder[Double](rows= users, cols= users)
    for (x <- topku) {
      for (y <- x._2) {
        builder.add(x._1,y._1,y._2)
      }
    }
    val mat = builder.result()
    var r_u : Double = 0.0
    ((u,i) => {
    var r_i = kNNRating(u-1,i-1,normalizedMatrix,mat,users,ratings)
    if (u-1 >= 0 && u-1 < users) {r_u = avgMatrix(u-1,0)} else avg
    r_u + r_i * scale(r_u + r_i, r_u)
    })
  }

  // Part 5: Distributed Approximate k-NN

  def createPartMat(list_user : Set[Int], br : org.apache.spark.broadcast.Broadcast[CSCMatrix[Double]], users : Int, movies : Int) : CSCMatrix[Double] = {
    val preProcessedMatrix = br.value
    val builder = new CSCMatrix.Builder[Double](rows = users, cols = movies)
    list_user.map(u=> (0 to movies-1).map(i=>builder.add(u,i, preProcessedMatrix(u,i))))
    builder.result()
  }

  def simPartition(k : Int, list_user : Set[Int], users : Int,movies : Int, br : org.apache.spark.broadcast.Broadcast[CSCMatrix[Double]]) : Seq[(Int,Seq[(Int,Double)])] = {
      val partMat = createPartMat(list_user, br,users,movies)
      var simMat = prodMat(partMat,users,movies)
      (list_user.toSeq).map(u => (u,argtopk(simMat(u,0 to users-1).t,k+1).filter(_ != u).map(v=>(v,simMat(u,v)))))
  }

  def simApprox(preProcessedMatrix : CSCMatrix[Double], k: Int, sc : SparkContext, partitions : Seq[Set[Int]], users : Int, movies : Int): CSCMatrix[Double] = {
    val br = sc.broadcast(preProcessedMatrix)
    var partitionRDD = sc.parallelize(partitions).flatMap(p=> simPartition(k, p, users,movies,br)).reduceByKey{case (x,y) => x.union(y).distinct}.collect()
    val builder = new CSCMatrix.Builder[Double](rows = users, cols = users)
    for (listu <- partitionRDD) {
      for (listv <- listu._2.sortBy(x => x._2)(Ordering.Double.reverse).take(k)) {
        builder.add(listu._1, listv._1, listv._2)
      }
    }
    val simMat = builder.result()
    simMat
  }

  def approximateKNN(ratings : CSCMatrix[Double], k: Int, sc : SparkContext, partitions : Seq[Set[Int]], users :Int, movies : Int): ((Int,Int) => Double) = {
    val avg = avgRating(ratings)
    val avgMatrix = avgRatingUserMatrix(ratings,users,movies)
    val normalizedMatrix = normalizedDevMatrix(ratings,avgMatrix,users,movies)
    val preProcessedMatrix = processedMatrix(normalizedMatrix,users,movies)
    val simMat = simApprox(preProcessedMatrix,k,sc,partitions,users,movies)
    var r_u : Double = 0.0
    ((u,i) => {
    var r_i = kNNRating(u-1,i-1,normalizedMatrix,simMat,users,ratings)
    if (u-1 >= 0 && u-1 < users) {r_u = avgMatrix(u-1,0)} else avg
    r_u + r_i * scale(r_u + r_i, r_u)
    })
  }

}


