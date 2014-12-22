
import akka.actor._
import scala.concurrent.duration.Duration
import scala.concurrent.duration._
import akka.routing._
import scala.util.Random

object project2 {

  class CustomRandom(i: Int) {
	var A = new Random(i)
	var B = new Random(i)
	var C = new Random(i)

	def generateRandom: Int = {
	  var j = 0
	  for ( k <- 0 until 5)
		j = (A.nextInt(i) + B.nextInt(i) + C.nextInt(i))/3
		return j
	}
  }
  
  var number_of_nodes_offline = 0
	
  def main(args: Array[String]) {

	if ( args.isEmpty || args.length < 3) {
	  println("usage: scala project2.scala <NumNodes> <Topology> <Algorithm>")
	  return
	}
	createTopology(args(0).toInt, args(1), args(2))	
  }

  sealed trait Message
  case object CreateTopology extends Message
  case object Converge extends Message
  case class WentOffline(i: Int) extends Message
  case class SendMessage(s: Double, w: Double) extends Message
  case object SendMessage extends Message
  case object StartMessage extends Message
  case object StartGossip extends Message
  case object Stop extends Message

  class Converger( numNodes: Int, topology: String, algorithm: String) extends Actor {
	var startTime: Long = 0
	var endTime: Long = 0

	//Finds all the valid neighbours of node i for Line
	def find_neighbors_line(i: Int, numNodes: Int): List[Int] = {
	  var list = List[Int]()
	  if ( i == 1) {
	    list::=( i + 1 )					  
	  }
	  else if ( i == numNodes ) {
	    list::=( i - 1 )						  
	  }
	  else {
	    list::=( i + 1 )
	    list::=( i - 1 )
	 }
	 list   
    }
	
	//Finds all the valid neighbours of node i for Full
	def find_neighbors_full(i: Int, numNodes: Int): List[Int] = {
	  val list = List(numNodes)
	  list
	}
	
	//Finds all the valid neighbours of node i for 2D grid		
	def find_neighbors_2d(i: Int , numNodes: Int ): List[Int] = {
	  var list = List[Int](); 
	  val n =math.ceil((math.sqrt(numNodes.toDouble))).toInt
	  if( i == 1 ) {
	    list::=( i + 1 )
	    list::=( i + n )
	  }
      else if( i == n ) {
        list::=( i - 1 )
        list::=( i + n )
      }
	  else if ( i > 1 && i < n) {
	    list::=( i - 1 )
		list::=( i + n )
		list::=( i + 1)
	  }
	  else if ( i == (n*n)-n+1 ) {
	    list::=( i + 1 )
	    list::=( i - n )
	  }
	  else if ( i == n*n ){
	    list::=( i - 1 )
	    list::=( i - n )
	  }
	  else if ( i > ((n*n)-n+1) && i < (n*n)){
	    list::=(i+1)
	    list::=(i-1)
	    list::=(i-n)
	  }
	  else if ( i > 1 && i < n*n+1 && i % n == 1){
	    list::=(i+1)
	    list::=(i+n)
	    list::=(i-n)
	  }
	  else if ( i > n && i < n*n && i % n == 0){
	    list::=(i-1)
	    list::=(i+n)
	    list::=(i-n)
	  }
	  else {
	    list::=(i+1)
	    list::=(i-1)
	    list::=(i+n)
	    list::=(i-n)
	  }
	  list.filter((j: Int) => j <= numNodes)
	}
	
	//Finds all the valid neighbours of node i for Imperfect 2D grid
	def find_neighbors_imperfect_2d(i: Int, numNodes: Int): List[Int] = {
	  var list = find_neighbors_2d(i, numNodes)
	  var rand = new CustomRandom(numNodes)
	  var randNum = rand.generateRandom
	  randNum+=1
	  while ( list.exists( (j: Int) => j == randNum) || i == randNum  ) {
	    randNum = rand.generateRandom
	    randNum+=1
	  }
	  list::=randNum
	  list
	}
	
	def find_neighbor(i: Int, numNodes: Int, topology: String): List[Int] = topology match {
	  case "full" => find_neighbors_full(i,numNodes)
	  case "2D" => find_neighbors_2d(i,numNodes)
	  case "imp2D" => find_neighbors_imperfect_2d(i, numNodes)
	  case "line" => find_neighbors_line(i,numNodes)
	}
	
	def receive = {
	  case `CreateTopology` => {
	    var actorList = List[ActorRef]()
	    for(i <- 0 until numNodes){
	      var neighborList = find_neighbor(i+1, numNodes, topology)
	      algorithm match {
	        case "gossip" =>
	          actorList::=(context.actorOf(Props(new GossipNode(topology, algorithm, neighborList, numNodes)),name = (i+1).toString))
	        case "push-sum" =>
	          actorList::=(context.actorOf(Props(new PushSumNode(topology, algorithm, neighborList, numNodes)),name = (i+1).toString))
	      }
	    }
	    var rand = new CustomRandom(numNodes)
	    var randNum =rand.generateRandom
	    startTime = System.currentTimeMillis
	    algorithm match {
	      case "gossip" =>
	        actorList(randNum) ! SendMessage
	      case "push-sum" =>
	        actorList(randNum) ! StartMessage
	    }
	  }
	  case Converge =>
	    var convergenceTime = System.currentTimeMillis - startTime
	    println("Convergence Time : "+convergenceTime)
	    context.system.shutdown
	  case _ =>
	    println("default Message **************")
    }   
  }
  
  class PushSumNode(topology: String, algorithm: String, list: List[Int], numNodes: Int) extends Actor {
    var s_i: Double = self.path.name.toInt
    var w_i: Double = 1
    var convergence_i: Double = 0.0
    var neighborList = list
    var deadList = List[Int]()
    var converge_counter : Int = 0
    var convergence_difference: Double = 0.0
    var received = 0
    
    override def preStart() {
      val interval = (self.path.name.toInt % 5) * 10
      import context.dispatcher
      context.system.scheduler.schedule(0 milliseconds, interval milliseconds, self, StartGossip)
    }
    
    def go_offline(i: Int) ={
      topology match {
        case "full" =>
          for( j <- 0 until neighborList(0) ){
            context.actorSelection("../"+(j+1)) ! WentOffline(i)
          }
          self ! Stop
          
        case "imp2D" =>
          context.actorSelection("../*") ! WentOffline(i)
          self ! Stop
          
        case _ =>
          for( j <- 0 until neighborList.length ){
            context.actorSelection("../"+neighborList(j)) ! WentOffline(i)
          }
          self ! Stop
      }
	}
    
    def select_random_neighbor(neighborList: List[Int]): Int = {
      var rand = new Random()
      topology match {
        case "full" =>
          var temp = rand.nextInt(neighborList(0))
          temp+=1
          return temp
        case _ =>
          var temp = rand.nextInt(neighborList.length)
          return neighborList(temp)
      }
	}
    
    def receive = {
      case `StartMessage` => {
        var receiver = context.actorSelection("../"+select_random_neighbor(neighborList))
        receiver ! SendMessage(s_i,w_i)     
				
	  }
      
      case SendMessage(s,w) => {
        if(number_of_nodes_offline >= numNodes) {
          context.actorSelection("../*") ! Stop
          self ! Stop
        }
        else {
          if( neighborList.isEmpty ) {
            number_of_nodes_offline+=1
            self ! Stop
          }
          
          else {
            var s_new = s_i + s
            var w_new = w_i + w
            var convergence_new = (s_new / w_new).toDouble
            convergence_i = (s_i / w_i).toDouble
            convergence_difference = convergence_new - convergence_i
            val value = math.pow(10,-10)
            
            if( convergence_difference < value ) {
              converge_counter = converge_counter.+(1)
            }
          
            else {
              converge_counter = 0
            }
          
            if(converge_counter == 3) {
              go_offline(self.path.name.toInt)
            }
          
            else {
              s_i = s_new/2
              w_i = w_new/2
              received+=1
		    }
          }          
        }
	  }
      
      case WentOffline(i: Int) => {
        if(number_of_nodes_offline >= numNodes) {
          //Tell neighbors that we need to converge
          context.actorSelection("../*") ! Stop
        }
        else {
          topology match {
            case "full" =>
              deadList::=(i)
              neighborList(0).-(1)
            case _ =>
              neighborList = neighborList.filter(( j: Int) => j != i)
          }
          if(neighborList.isEmpty || (deadList.length >= (numNodes - 1))){
            number_of_nodes_offline+=1
            self ! Stop
          }
        } 
      }
      
      case `StartGossip` => {
        if(number_of_nodes_offline >= numNodes) {
          //Tell neighbors that we need to converge
          context.actorSelection("../*") ! Stop
          self ! Stop
        }
        else if( neighborList.isEmpty || (deadList.length >= (numNodes - 1))) {
          number_of_nodes_offline+=1
          self ! Stop
        }
        else {
          if (received > 0) {
            val value = math.pow(10,-10)
            if( convergence_difference < value ) {
              converge_counter = converge_counter.+(1)
            }          
            else {
              converge_counter = 0
            }          
            if(converge_counter == 3) {
              go_offline(self.path.name.toInt)
            }
            else{
              var receiver = context.actorSelection("../"+select_random_neighbor(neighborList))
              receiver ! SendMessage(s_i,w_i)
            }
          }
        }
      }
      
      case `Stop` => {
        if(number_of_nodes_offline >= numNodes) {
          context.actorSelection("..") ! Converge
          context.stop(self)
        }
      }
			
	  case _ => println("default case **********************")
	}
  }
  
  class GossipNode(topology: String, algorithm: String, list: List[Int], numNodes: Int) extends Actor {
    var messageLife: Int = 0
    var convergeCount: Int = 10
    var neighborList = list
    var deadList = List[Int]()
    
    override def preStart() {
      val interval = (self.path.name.toInt % 5) * 10
      import context.dispatcher
      context.system.scheduler.schedule(0 milliseconds, interval milliseconds, self, StartGossip)
    }
    
    def go_offline(i: Int) = {
      topology match {
        case "full" =>
          for( j <- 0 until neighborList(0) ){
            context.actorSelection("../"+(j+1)) ! WentOffline(i)
          }
          self ! Stop
          
        case _ =>
          for( j <- 0 until neighborList.length ){
            context.actorSelection("../"+neighborList(j)) ! WentOffline(i)
          }
          self ! Stop
      }
	}
    
    def select_random_neighbor(neighborList: List[Int]): Int = {
      var rand = new Random()
      topology match {
        case "full" =>
          var temp = rand.nextInt(neighborList(0))
          temp+=1
          return temp
        case _ =>
          var temp = rand.nextInt(neighborList.length)
          return neighborList(temp)
      }
	}
    
    def receive = {
      case `SendMessage` => {
        if(number_of_nodes_offline >= numNodes) {
          context.actorSelection("../*") ! Stop
          self ! Stop
        }
        else {
                    
          if( neighborList.isEmpty ) {
            number_of_nodes_offline+=1
            self ! Stop
          }
          
          else if( messageLife == convergeCount ) {
		    go_offline(self.path.name.toInt)
		    //tell neighbors that you will be going offline
		  }
          else
            messageLife = messageLife + 1          
        }
      }	
      
      case WentOffline(i: Int) => {
        if(number_of_nodes_offline >= numNodes) {
          //Tell neighbors that we need to converge
          context.actorSelection("../*") ! Stop
        }
        else {
          topology match {
            case "full" =>
              deadList::=(i)
              neighborList(0).-(1)
            case _ =>
              neighborList = neighborList.filter(( j: Int) => j != i)
          }
          if(neighborList.isEmpty) {
            number_of_nodes_offline+=1
            self ! Stop
          }
        }        
      }
      case `StartGossip` => {
        if(number_of_nodes_offline >= numNodes) {
          //Tell neighbors that we need to converge
          context.actorSelection("../*") ! Stop
          self ! Stop
        }
        else if( neighborList.isEmpty ) {
          number_of_nodes_offline+=1
          self ! Stop
        }
        else if( messageLife == convergeCount ) {
          number_of_nodes_offline+=1
		  go_offline(self.path.name.toInt)
		}        
        else {
          topology match {
            case "full" => {
              if((messageLife > 0) && (messageLife < convergeCount)) {
                var receiver = context.actorSelection("../"+select_random_neighbor(neighborList))
			    receiver ! SendMessage
			  }
		    }
            case _ => {
              if((messageLife > 0) && (messageLife < convergeCount)) {
                var receiver = context.actorSelection("../"+select_random_neighbor(neighborList))
			    receiver ! SendMessage
			  }
		    }
          }
        }
      }
      
      case `Stop` => {
        if(number_of_nodes_offline >= numNodes) {
          context.actorSelection("..") ! Converge
          context.stop(self)
        }
      }
      case _ => 
        println("default case **********************")
    }
  }
  
  def createTopology(numNodes: Int, topology: String, algorithm: String) {
    // Create an Akka  Actor system
    val system = ActorSystem("Proj2System")
    
    // create the master
	val converger = system.actorOf(Props(new Converger(numNodes, topology, algorithm)),name = "converger")

	// start the calculation
	converger ! CreateTopology
  }
}