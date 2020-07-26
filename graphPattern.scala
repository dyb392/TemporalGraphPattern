import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD


// Assume the SparkContext has already been constructed
val sc: SparkContext
// Create an RDD for the vertices
val users: RDD[(VertexId, (String, String))] =
  sc.parallelize(Array((3L, ("rxin", "student")), (7L, ("jgonzal", "postdoc")),
                       (5L, ("franklin", "prof")), (2L, ("istoica", "prof"))))
// Create an RDD for edges
val relationships: RDD[Edge[String]] =
  sc.parallelize(Array(Edge(3L, 7L, "collab"),    Edge(5L, 3L, "advisor"),
                       Edge(2L, 5L, "colleague"), Edge(5L, 7L, "pi")))
// Define a default user in case there are relationship with missing user
val defaultUser = ("John Doe", "Missing")
// Build the initial Graph
val graph = Graph(users, relationships, defaultUser)


import org.apache.spark._
import org.apache.spark.graphx._
// To make some of the examples work we will also need RDD
import org.apache.spark.rdd.RDD
val studentsPattern: RDD[(VertexId, String)] = 
    sc.parallelize(Array((1L, "s"), (2L, "u"), (3L, "d"), (4L, "v"),
                        (5L, "w"), (6L, "c")))

val timesPattern: RDD[Edge[(Int, Int)]] = 
    sc.parallelize(Array(Edge(1L, 2L, (10900, 11000)), Edge(2L, 3L, (11400, 11500)),
                        Edge(1L, 4L, (10700, 10800)), Edge(2L, 4L, (40800, 40900)),
                        Edge(2L, 5L, (21500, 21600)), Edge(2L, 6L, (21000, 21100))))

val patternGraph = Graph(studentsPattern, timesPattern)

val studentsData: RDD[(VertexId, (String, String))] = 
    sc.parallelize(Array((1L, ("Dan", "s")), (2L, ("Jane", "u")), (3L, ("Sclina", "d")), (4L, ("Pat", "v")),
                        (5L, ("Ross", "w")), (6L, ("Jenny", "c")), (7L, ("Gray", "s")), (8L, ("Mark", "u")),
                        (9L, ("Gaia", "d")), (10L, ("Jessica", "v")), (11L, ("Bill", "w"))))

val timesData: RDD[Edge[(Int, Int)]] = 
    sc.parallelize(Array(Edge(1L, 2L, (10900, 10940)), Edge(2L, 3L, (11400, 11420)),
                        Edge(1L, 4L, (10650, 10800)), Edge(2L, 4L, (40800, 40840)),
                        Edge(2L, 5L, (21430, 21500)), Edge(2L, 6L, (21000, 21100)),
                        Edge(1L, 5L, (51800, 52000)), Edge(7L, 2L, (50700, 50800)),
                        Edge(7L, 10L, (11300, 11400)), Edge(8L, 4L, (20800, 20900)),
                        Edge(8L, 3L, (11500, 11600)), Edge(8L, 6L, (31300, 31400)),
                        Edge(8L, 9L, (30800, 30900)), Edge(8L, 11L, (51300, 51400)),
                        Edge(10L, 6L, (11600, 11700)), Edge(10L, 9L, (21500, 21600))))

val dataGraph = Graph(studentsData, timesData)

dataGraph.vertices.filter { case (id, (name, level)) => id > 0}.collect.foreach {
  case(id, (name, level)) => println(s"$name's level is $level")
}

