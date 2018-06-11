package akka.stream.checkpoint.benchmarks

import java.util.concurrent.TimeUnit

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.checkpoint.{CheckpointBackend, CheckpointRepository, DropwizardBackend, KamonBackend}
import akka.stream.checkpoint.scaladsl.Checkpoint
import akka.stream.scaladsl.{Flow, Keep, RunnableGraph, Sink, Source}
import com.codahale.metrics.MetricRegistry
import org.openjdk.jmh.annotations._

import scala.concurrent._
import scala.concurrent.duration._

/*
[info] Benchmark                                               (numberOfFlows)  (repositoryType)   Mode  Cnt      Score     Error   Units
[info] CheckpointBenchmark.map_with_checkpoints_100k_elements                1              none  thrpt   20  13184.014 ± 139.389  ops/ms
[info] CheckpointBenchmark.map_with_checkpoints_100k_elements                1              noop  thrpt   20   5414.791 ±  64.548  ops/ms
[info] CheckpointBenchmark.map_with_checkpoints_100k_elements                1        dropwizard  thrpt   20   1278.878 ±  87.234  ops/ms
[info] CheckpointBenchmark.map_with_checkpoints_100k_elements                1             kamon  thrpt   20   4584.435 ±  31.389  ops/ms
[info] CheckpointBenchmark.map_with_checkpoints_100k_elements                5              none  thrpt   20   5642.095 ± 118.584  ops/ms
[info] CheckpointBenchmark.map_with_checkpoints_100k_elements                5              noop  thrpt   20   1291.007 ±  54.667  ops/ms
[info] CheckpointBenchmark.map_with_checkpoints_100k_elements                5        dropwizard  thrpt   20    260.106 ±   5.408  ops/ms
[info] CheckpointBenchmark.map_with_checkpoints_100k_elements                5             kamon  thrpt   20   1012.013 ±  33.434  ops/ms
[info] CheckpointBenchmark.map_with_checkpoints_100k_elements               20              none  thrpt   20   1829.584 ±  28.066  ops/ms
[info] CheckpointBenchmark.map_with_checkpoints_100k_elements               20              noop  thrpt   20    329.244 ±  23.158  ops/ms
[info] CheckpointBenchmark.map_with_checkpoints_100k_elements               20        dropwizard  thrpt   20     66.274 ±   1.947  ops/ms
[info] CheckpointBenchmark.map_with_checkpoints_100k_elements               20             kamon  thrpt   20    233.347 ±   4.157  ops/ms
*/
@State(Scope.Benchmark)
@OutputTimeUnit(TimeUnit.MILLISECONDS)
@BenchmarkMode(Array(Mode.Throughput))
class CheckpointBenchmark {

  implicit val system = ActorSystem("CheckpointBenchmark")
  implicit val materializer = ActorMaterializer()
  implicit val ctx = system.dispatcher

  implicit val metricRegistry = new MetricRegistry()

  val noopBackend = new CheckpointBackend {
    override def createRepository(name: String): CheckpointRepository = new CheckpointRepository {
      override def markPush(latencyNanos: Long, backpressureRatio: Long): Unit = ()
      override def markPull(latencyNanos: Long): Unit = ()
    }
  }

  val numberOfElements = 100000

  @Param(Array("1", "5", "20"))
  var numberOfFlows = 1

  @Param(Array("none", "noop", "dropwizard", "kamon"))
  var repositoryType = "none"

  var graph: RunnableGraph[Future[Done]] = _

  @Setup
  def setup(): Unit = {
    val source = Source.repeat(1).take(numberOfElements)
    val flow = Flow[Int].map(_ + 1)

    def viaCheckpoint(n: Int) = {
      def withBackend(backend: CheckpointBackend) = flow.via(Checkpoint[Int](n.toString)(backend))

      repositoryType match {
        case "none" ⇒ flow
        case "noop" ⇒ withBackend(noopBackend)
        case "dropwizard" ⇒ withBackend(DropwizardBackend.fromRegistry)
        case "kamon" ⇒ withBackend(KamonBackend.instance)
      }
    }

    val flows = (1 to numberOfFlows).map(viaCheckpoint).reduce(_ via _)
    graph = source.via(flows).toMat(Sink.ignore)(Keep.right)
  }

  @TearDown
  def shutdown(): Unit = {
    Await.result(system.terminate(), 5.seconds)
  }

  @Benchmark
  @OperationsPerInvocation(100000) // Note: needs to match NumberOfElements.
  def map_with_checkpoints_100k_elements(): Unit = {
    Await.result(graph.run(), Duration.Inf)
  }
}
