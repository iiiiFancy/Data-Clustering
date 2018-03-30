import java.io.{File, FileWriter}

import scala.io.Source

object Chang_Fan_clustering {
  def main(args: Array[String]) {
    var i = 0
    val k = args(1).toInt
    var input: List[String] = List()
    val file = Source.fromFile(args(0))
    val writer = new FileWriter(new File("Chang_Fan_result_cluster_" + args(1) + ".txt"))
    for (line <- file.getLines) {
      val temp = line + "," + i.toString
      input = input :+ temp
      i = i + 1
    }
    file.close

    val len = input.length
    var splitedInput = input.map(x => (List(x.split(",")(5).toInt), x.split(",")(0).toDouble, x.split(",")(1).toDouble,
      x.split(",")(2).toDouble, x.split(",")(3).toDouble, x.split(",")(4))) // List(Object); all points left
    val backup = input.map(x => (x.split(",")(5).toInt, x.split(",")(0).toDouble, x.split(",")(1).toDouble,
      x.split(",")(2).toDouble, x.split(",")(3).toDouble, x.split(",")(4)))

    var pair: List[((List[Int], Double, Double, Double, Double, String),
      (List[Int], Double, Double, Double, Double, String))] = List()
    for (i <- 0 until len) {
      for (j <- i + 1 until len) {
        val temp2 = (splitedInput(i), splitedInput(j))
        pair = pair :+ temp2
      }
    }
    //pair.foreach(x => println(x))

    val distance = pair.map(x => // (#1, #2, distance)
      (x._1._1, x._2._1, Math.sqrt((x._1._2 - x._2._2) * (x._1._2 - x._2._2)
        + (x._1._3 - x._2._3) * (x._1._3 - x._2._3)
        + (x._1._4 - x._2._4) * (x._1._4 - x._2._4))
        + (x._1._5 - x._2._5) * (x._1._5 - x._2._5)))
    //distance.foreach(x => println(x))

    implicit val ord: Ordering[(List[Int], List[Int], Double)] = Ordering.by(_._3)
    val priorityQueue = collection.mutable.PriorityQueue[(List[Int], List[Int], Double)]().reverse
    distance.foreach(x => priorityQueue.enqueue(x))

    while (splitedInput.size > k) {
      val min = priorityQueue.dequeue()

      // check if the two points in this pair are still there
      val clusterA = splitedInput.filter(x => x._1 == min._1)
      val clusterB = splitedInput.filter(x => x._1 == min._2)

      if (clusterA.nonEmpty & clusterB.nonEmpty) { // if they are, remove and merge them
        //println("effective pair")
        val temp3 = splitedInput.diff(clusterA).diff(clusterB)
        splitedInput = temp3
        // compute centroid
        val sizeOfA = clusterA.head._1.size
        val sizeOfB = clusterB.head._1.size
        val newX1: List[Int] = (min._1 ++ min._2).sorted
        val newX2: Double = (clusterA.head._2 * sizeOfA + clusterB.head._2 * sizeOfB) / (sizeOfA + sizeOfB)
        val newX3: Double = (clusterA.head._3 * sizeOfA + clusterB.head._3 * sizeOfB) / (sizeOfA + sizeOfB)
        val newX4: Double = (clusterA.head._4 * sizeOfA + clusterB.head._4 * sizeOfB) / (sizeOfA + sizeOfB)
        val newX5: Double = (clusterA.head._5 * sizeOfA + clusterB.head._5 * sizeOfB) / (sizeOfA + sizeOfB)
        val newX6: String = clusterA.head._6.concat(",").concat(clusterB.head._6)
        val newCluster = (newX1, newX2, newX3, newX4, newX5, newX6)
        //println(newCluster)

        // compute new distance
        val len2 = splitedInput.length
        var newToAllOthers: List[((List[Int], Double, Double, Double, Double, String),
          (List[Int], Double, Double, Double, Double, String))] = List()
        for (i <- 0 until len2) {
          val temp4 = (newCluster, splitedInput(i))
          newToAllOthers = newToAllOthers :+ temp4
        }
        val newDistance = newToAllOthers.map(x => // (#1, #2, distance)
          (x._1._1, x._2._1, Math.sqrt((x._1._2 - x._2._2) * (x._1._2 - x._2._2)
            + (x._1._3 - x._2._3) * (x._1._3 - x._2._3)
            + (x._1._4 - x._2._4) * (x._1._4 - x._2._4))
            + (x._1._5 - x._2._5) * (x._1._5 - x._2._5)))
        //newDistance.foreach(x => println(x))
        newDistance.foreach(x => priorityQueue.enqueue(x)) // add new distance into queue

        // add new cluster into points left
        val temp5 = splitedInput :+ newCluster
        splitedInput = temp5
      }
    }

    // decide cluster name
    // splitedInput.foreach(x => println(x))
    var wrong = 0
    splitedInput.foreach(x => { // for each cluster
      val numberList = x._1
      var allNames: List[String] = List() // count appearances of names
      var eachCluster: List[Product] = List() // separated cluster
      numberList.foreach(y => { // recover data from sequence number to full information
        val pointsInf = backup.filter(z => z._1 == y).map(u => (u._2, u._3, u._4, u._5, u._6)) // remove seq number
        //println(pointsInf)
        allNames = allNames :+ pointsInf.head._5
        eachCluster = eachCluster :+ pointsInf
      })

      val names = allNames.distinct // all kinds of names
      var max = 0
      var clusterName = "Have a nice day!"
      names.foreach(v => { // for each kind of name
        val numberOfThisName = allNames.count(x => x.contains(v))
        if (numberOfThisName > max) {
          max = numberOfThisName
          clusterName = v
        }
      })
      wrong = wrong + allNames.size - max
      writer.write("Cluster: " + clusterName + "\n")
      eachCluster.foreach(x => writer.write(x.toString.substring(5, x.toString.length - 1) + "\n"))
      writer.write("Number of points in this cluster: " + x._1.size.toString + "\n" + "\n")
    })
    writer.write("Number of points wrongly assigned: " + wrong)
    writer.close()
  }
}