package akka.stream.checkpoint

import java.util.concurrent.atomic.AtomicLong

import akka.NotUsed
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Keep, Sink, Source}
import akka.stream.testkit.scaladsl.TestSink
import org.scalatest.concurrent.{Eventually, ScalaFutures}
import org.scalatest.{MustMatchers, WordSpec}

import scala.collection.mutable.ListBuffer

class CheckpointStageSpec extends WordSpec with MustMatchers with ScalaFutures with Eventually {

  implicit val system = ActorSystem("CheckpointSpec")
  implicit val materializer = ActorMaterializer()

  val counterClock = new Clock {
    val count = new AtomicLong(0L)
    override def nanoTime: Long = count.incrementAndGet()
  }

  "The Checkpoint stage" should {
    "be a pass-through stage" in {

      val noopRepo = new CheckpointRepository {
        override def markPull(latencyNanos: Long): Unit = ()
        override def markPush(latencyNanos: Long, backpressureRatio: Long): Unit = ()
      }

      val values = 1 to 5

      val results = Source(values).via(CheckpointStage(noopRepo, counterClock)).runWith(Sink.seq).futureValue

      results must ===(values)
    }

    "record latencies between pulls and pushes" in {
      val pushLatencies = ListBuffer.empty[Long]
      val pullLatencies = ListBuffer.empty[Long]

      val arrayBackedRepo = new CheckpointRepository {
        override def markPush(nanos: Long, backpressureRatio: Long): Unit = pushLatencies += nanos
        override def markPull(nanos: Long): Unit = pullLatencies += nanos
      }

      val (sourcePromise, probe) =
        Source.maybe[Int]
          .via(CheckpointStage(arrayBackedRepo, counterClock))
          .toMat(TestSink.probe[Int])(Keep.both)
          .run()

      pullLatencies.size must ===(0)
      pushLatencies.size must ===(0)

      probe.request(1)

      eventually {
        pullLatencies.size must ===(1)
        pushLatencies.size must ===(0)

        pullLatencies.head must ===(1)
      }

      sourcePromise.success(Some(42))

      eventually {
        pullLatencies.size must ===(1)
        pushLatencies.size must ===(1)

        pushLatencies.head must ===(1)
      }

      probe.expectNext(42)
    }

    "record backpressure ratios at push time" in {
      val backpressureRatios = ListBuffer.empty[Long]

      val arrayBackedRepo = new CheckpointRepository {
        override def markPush(nanos: Long, backpressureRatio: Long): Unit = backpressureRatios += backpressureRatio
        override def markPull(nanos: Long): Unit = ()
      }

      val probe =
        Source.single(NotUsed)
          .via(CheckpointStage(arrayBackedRepo, counterClock))
          .runWith(TestSink.probe[NotUsed.type])

      eventually {
        backpressureRatios.size must ===(0)
      }

      probe.request(1)

      eventually {
        backpressureRatios.size must ===(1)
        backpressureRatios.head must ===(50)
      }
    }
  }

}
