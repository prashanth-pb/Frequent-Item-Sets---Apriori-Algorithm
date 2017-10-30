import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import scala.math.Ordering.Implicits._
import java.io._

import scala.collection.mutable.ListBuffer

object Case1 {

  def apriori_calc(data: List[List[String]], cmps: List[String], sup: Double, mc: Double) :Map[List[String],Int]= {
    var condition = true
    val sz = data.size
    val double = sup * (sz.toDouble/mc)
    var finalList = ListBuffer[(List[String], Int)]()
    var k = 2
    var cmn = List[(List[String])]()
    var candidates = cmps.sorted.combinations(k).map(x => x.toSet).toList
    val data1 = data.map(x => x.toSet)
    while (condition) {
      var count = 0
      val ans = candidates.map(c => {
        count = 0
        data1.map(d => {
          if (c.subsetOf(d)) count += 1
        })
        (c.toList, count)
      })
      //println("Before Filter : " + ans.size)
      val p = ans.filter(elem => elem._2.toDouble >= double)

      if (p.size !=0) {
        p.map(x => {
          finalList.append(x)})
        cmn = p.map(x => x._1)
        k = k+1
        candidates = cmn.flatten.distinct.sorted.combinations(k).map(x => x.toSet).toList
      }
      else {
        condition = false
      }
      //println("After Filer :" + finalList.size)
    }
    finalList.toMap


    //data.map(x => x)
  }

  def final_calc(data: List[List[String]], cmps: Array[List[String]], sup:Double, mc: Double) :Map[List[String],Int]= {
    val cands = cmps.map(x => x.toSet).toList
    //cands.take(5).foreach(println)
    val sz = data.size
    val double = sup * (sz.toDouble/mc)
    var count = 0
    val ans = cands.map(c => {
      count = 0
      val data1 = data.map(x => x.toSet)
      data1.map(d => {
        if (c.subsetOf(d)) count += 1
      })
      (c.toList,count)
    })
    //println("Before Filter : "+ ans.size)
    val finalList = ans.toMap
    //println("After Filer :" + finalList.size)
    finalList


    //data.map(x => x)
  }

  def main(args: Array[String]): Unit = {
    //    val sparkConf = new SparkConf().setAppName("Task11").setMaster("local[2]")
    //    val sc = new SparkContext(sparkConf)
    //println(args(0))
    //println(args(1))
    val mybask = List[(List[String])]()
    val u1 = Seq[(Int, String)]()
    val glist = ListBuffer[(List[List[Int]])]()
    val support = args(3).toInt
    val sparkConf = new SparkConf().setAppName("SparkWordCount").setMaster("local[*]")
    val sc = new SparkContext(sparkConf)
    var baskets = sc.parallelize(mybask)
    var user_data_male = sc.parallelize(u1)
    var ratings_data = sc.parallelize(u1)
    val user_file = sc.textFile(args(2))
    val ratings_file = sc.textFile(args(1))
    if (args(0).toInt == 1) {
      user_data_male = user_file.map(line => line.split("::")).map(arr => (arr(0).toInt, arr(1))).filter(_._2.contains("M"))

      ratings_data = ratings_file.map(line => line.split("::")).map(arr => (arr(0).toInt, arr(1)))

      baskets = user_data_male.join(ratings_data).map(arr => (arr._1, arr._2._2)).groupBy(_._1).mapValues(_.map(_._2)(collection.breakOut).toList.distinct).map(k => k._2)
    }

    else if(args(0).toInt == 2) {
      user_data_male = user_file.map(line => line.split("::")).map(arr => (arr(0).toInt, arr(1))).filter(_._2.contains("F"))
      ratings_data = ratings_file.map(line => line.split("::")).map(arr => (arr(0).toInt, arr(1)))
      baskets = user_data_male.join(ratings_data).map(arr => (arr._2._2, arr._1.toString)).groupBy(_._1).mapValues(_.map(_._2)(collection.breakOut).toList.distinct).map(k => k._2)

    }
    val new_basket = baskets.flatMap(x => x.map(y => (y, 1)))

    //val work1 = sc.parallelize(new_basket.collect(), 5)
    val form_singles = new_basket.reduceByKey((x,y) => x+y).collect()
    val singletons = form_singles.filter(_._2 >= support)
    val singles = singletons.map(arr => arr._1).toList
    val baskets2 = sc.parallelize(baskets.collect().toList)
    val cn = baskets2.count()
    //baskets2.foreach(arr => println(arr))
    val mapped = baskets2.mapPartitionsWithIndex((index, iterator) => {
      //println("Called in Partition -> " + index)
      //val test = iterator.map(x => x)
      apriori_calc(iterator.toList, singles, support, cn).iterator
    } ).reduceByKey((a,b)=>1)
    val mapped2 = mapped.map(x => x._1).collect()
   val final_ans = baskets2.mapPartitionsWithIndex((index, iterator) => {
      //println("Called in final Partition -> " + index)
      final_calc(iterator.toList, mapped2, support, cn).iterator
    }).reduceByKey((a,b)=>a+b)
    val ff1 = final_ans.filter(_._2 >= support).map(x => x._1).collect().toList
    val test3 = singles.map(x => List(x))
    val ff = List.concat(ff1, test3)
    val ff3 = ff.map(x => x.map(_.toInt)).map(y => y.sorted)
    //ff.foreach(println)
    //println("Final count" + ff.count())
    //ff.foreach(println)
    var cond = true
    var k = 1
    while(cond) {
      var temp = ff3.filter(_.size == k).sorted
      //temp.foreach(println)
      if (temp.size !=0) {
        glist.append(temp)
        k = k +1
      }
      else {
        cond = false
      }
    }
    var strl = glist.map(x => x.map(y => y.mkString("(", ",", ")"))).mkString("\n")
    val strl1 = strl.replaceAll("List\\(", "")
    val strl2 = strl1.replaceAll("\\)\n", "\n").dropRight(1)
    val pw = new PrintWriter(new File("output.txt" ))
    pw.write(strl2)
    pw.close()
    //print("Hello")


  }
}