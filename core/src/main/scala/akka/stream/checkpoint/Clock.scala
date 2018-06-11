package akka.stream.checkpoint

private[checkpoint] trait Clock {

  def nanoTime: Long
}

private[checkpoint] object SystemClock extends Clock {

  override def nanoTime: Long = System.nanoTime()
}
