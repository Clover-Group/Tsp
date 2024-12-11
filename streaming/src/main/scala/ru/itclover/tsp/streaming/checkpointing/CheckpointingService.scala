package ru.itclover.tsp.streaming.checkpointing

import com.typesafe.scalalogging.Logger
import org.redisson.Redisson
import org.redisson.config.Config
import ru.itclover.tsp.core.{RawPattern, Segment}
import ru.itclover.tsp.core.optimizations.Optimizer.{S => State}

import scala.collection.mutable
import ru.itclover.tsp.core.optimizations.Optimizer
import org.redisson.codec.JsonJacksonCodec
import java.time.Duration
import java.util.concurrent.ScheduledThreadPoolExecutor
import java.util.concurrent.TimeUnit
import java.util.concurrent.ScheduledFuture

trait CheckpointingService {
  def updateCheckpointRead(uuid: String, newRowsRead: Long, newStates: Map[RawPattern, State[Segment]]): Unit

  def updateCheckpointWritten(uuid: String, sinkIdx: Int, newRowsWritten: Long): Unit

  def getCheckpointAndState(uuid: String): (Option[Checkpoint], Option[CheckpointState])

  def getCheckpoint(uuid: String): Option[Checkpoint]

  def removeCheckpointAndState(uuid: String): Unit
  def setCheckpointAndStateExpirable(uuid: String): Unit
}

case class RedisCheckpointingService(redisUri: String) extends CheckpointingService {

  val redisConfig = new Config()

  val log = Logger("RedisCheckpointing")

  val codec = new JsonJacksonCodec()

  redisConfig
    .useSingleServer()
    .setConnectionMinimumIdleSize(16)
    .setConnectionPoolSize(32)
    .setAddress(redisUri)

  val redissonClient = Redisson.create(redisConfig)

  override def updateCheckpointRead(
    uuid: String,
    newRowsRead: Long,
    newStates: Map[RawPattern, State[Segment]]
  ): Unit = {
    val lock = redissonClient.getLock(uuid)
    lock.lock()
    val checkpointBucket = redissonClient.getBucket[Checkpoint](s"tsp-cp-$uuid", codec)
    val checkpoint = Option(checkpointBucket.get).getOrElse(Checkpoint(0, Array.empty))
    val newCheckpoint = checkpoint.copy(
      readRows = checkpoint.readRows + newRowsRead
    )
    val checkpointStateBucket = redissonClient.getBucket[CheckpointState](s"tsp-cp-$uuid-state")
    log.warn(s"Checkpointing $uuid: ${checkpoint.readRows + newRowsRead} rows read")
    checkpointBucket.set(newCheckpoint)
    checkpointStateBucket.set(CheckpointState(newStates))
    lock.unlock()
  }

  override def updateCheckpointWritten(uuid: String, sinkIdx: Int, newRowsWritten: Long): Unit = {
    val lock = redissonClient.getLock(uuid)
    lock.lock()
    val checkpointBucket = redissonClient.getBucket[Checkpoint](s"tsp-cp-$uuid", codec)
    val checkpoint = checkpointBucket.get()

    val writtenRows = checkpoint.writtenRows.padTo(sinkIdx + 1, 0L)
    val updatedWrittenRows = writtenRows.updated(sinkIdx, writtenRows(sinkIdx) + newRowsWritten)
    val newCheckpoint = checkpoint.copy(
      writtenRows = updatedWrittenRows
    )
    checkpointBucket.set(newCheckpoint)
    lock.unlock()
  }

  override def getCheckpointAndState(uuid: String): (Option[Checkpoint], Option[CheckpointState]) = {
    val checkpointBucket = redissonClient.getBucket[Checkpoint](s"tsp-cp-$uuid", codec)
    val checkpointStateBucket = redissonClient.getBucket[CheckpointState](s"tsp-cp-$uuid-state")
    log.warn(s"Restored checkpoint: ${checkpointBucket.get()}")
    (Option(checkpointBucket.get()), Option(checkpointStateBucket.get()))
  }

  override def getCheckpoint(uuid: String): Option[Checkpoint] = {
    val checkpointBucket = redissonClient.getBucket[Checkpoint](s"tsp-cp-$uuid", codec)
    Option(checkpointBucket.get())
  }

  override def removeCheckpointAndState(uuid: String): Unit = {
    val checkpointBucket = redissonClient.getBucket[Checkpoint](s"tsp-cp-$uuid", codec)
    val checkpointStateBucket = redissonClient.getBucket[CheckpointState](s"tsp-cp-$uuid-state")
    checkpointBucket.delete()
    checkpointStateBucket.delete()
  }

  override def setCheckpointAndStateExpirable(uuid: String): Unit = {
    val checkpointBucket = redissonClient.getBucket[Checkpoint](s"tsp-cp-$uuid", codec)
    val checkpointStateBucket = redissonClient.getBucket[CheckpointState](s"tsp-cp-$uuid-state")
    checkpointBucket.getAndExpire(Duration.ofHours(3))
    checkpointStateBucket.getAndExpire(Duration.ofHours(3))
  }

}

case class MemoryCheckpointingService() extends CheckpointingService {

  val log = Logger("MemoryCheckpointing")
  val scheduler = new ScheduledThreadPoolExecutor(8)

  private val checkpoints: mutable.Map[String, Checkpoint] = mutable.Map.empty
  private val checkpointStates: mutable.Map[String, CheckpointState] = mutable.Map.empty
  private val checkpointFutures: mutable.Map[String, ScheduledFuture[?]] = mutable.Map.empty

  override def updateCheckpointRead(
    uuid: String,
    newRowsRead: Long,
    newStates: Map[RawPattern, Optimizer.S[Segment]]
  ): Unit = {
    checkpoints.synchronized {
      val checkpoint = checkpoints.getOrElse(uuid, Checkpoint(0, Array.empty))
      val newCheckpoint = checkpoint.copy(
        readRows = checkpoint.readRows + newRowsRead
      )
      log.warn(s"Checkpointing $uuid: ${checkpoint.readRows + newRowsRead} rows read")
      checkpoints(uuid) = newCheckpoint
      checkpointStates(uuid) = CheckpointState(newStates)
    }
  }

  override def updateCheckpointWritten(uuid: String, sinkIdx: Int, newRowsWritten: Long): Unit = {
    checkpoints.synchronized {
      val checkpoint = checkpoints.getOrElse(uuid, Checkpoint(0, Array.empty))

      val writtenRows = checkpoint.writtenRows.padTo(sinkIdx + 1, 0L)
      val updatedWrittenRows = writtenRows.updated(sinkIdx, writtenRows(sinkIdx) + newRowsWritten)
      val newCheckpoint = checkpoint.copy(
        writtenRows = updatedWrittenRows
      )
      checkpoints(uuid) = newCheckpoint
    }
  }

  override def getCheckpointAndState(uuid: String): (Option[Checkpoint], Option[CheckpointState]) =
    (checkpoints.get(uuid), checkpointStates.get(uuid))

  override def getCheckpoint(uuid: String): Option[Checkpoint] = checkpoints.get(uuid)

  override def removeCheckpointAndState(uuid: String): Unit = {
    checkpoints.remove(uuid)
    checkpointStates.remove(uuid)
    checkpointFutures.get(uuid).map(_.cancel(true))
  }

  override def setCheckpointAndStateExpirable(uuid: String): Unit = {
    // TODO: Expirable?
    checkpointFutures(uuid) = scheduler.schedule(
      new Runnable {
        def run() = {
          checkpoints.remove(uuid)
          checkpointStates.remove(uuid)
          ()
        }
      },
      3,
      TimeUnit.HOURS
    )
  }

}

object CheckpointingService {
  private var service: Option[CheckpointingService] = None

  def getOrCreate(redisUri: Option[String]): CheckpointingService = service match {
    case Some(value) =>
      value
    case None =>
      val srv = redisUri match {
        case Some(uri) => RedisCheckpointingService(uri)
        case None      => MemoryCheckpointingService()
      }
      service = Some(srv)
      srv
  }

  def forceCreate(redisUri: Option[String]) = {
    val srv = redisUri match {
      case Some(uri) => RedisCheckpointingService(uri)
      case None      => MemoryCheckpointingService()
    }
    service = Some(srv)
    srv
  }

  def updateCheckpointRead(uuid: String, newRowsRead: Long, newStates: Map[RawPattern, State[Segment]]): Unit =
    service.map(_.updateCheckpointRead(uuid, newRowsRead, newStates)).getOrElse(())

  def updateCheckpointWritten(uuid: String, sinkIdx: Int, newRowsWritten: Long): Unit =
    service.map(_.updateCheckpointWritten(uuid, sinkIdx, newRowsWritten)).getOrElse(())

  def getCheckpointAndState(uuid: String): (Option[Checkpoint], Option[CheckpointState]) =
    service.map(_.getCheckpointAndState(uuid)).getOrElse((None, None))

  def getCheckpoint(uuid: String): Option[Checkpoint] =
    service.flatMap(_.getCheckpoint(uuid))

  def removeCheckpointAndState(uuid: String): Unit =
    service.map(_.removeCheckpointAndState(uuid)).getOrElse(())

  def setCheckpointAndStateExpirable(uuid: String): Unit =
    service.map(_.setCheckpointAndStateExpirable(uuid)).getOrElse(())

}
