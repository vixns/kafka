/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ly.stealth.mesos.kafka

import java.util
import java.util.{Date, UUID}
import org.apache.mesos.Protos.Resource.{DiskInfo, ReservationInfo}
import org.apache.mesos.Protos.Resource.DiskInfo.{Persistence, Source}
import org.apache.mesos.Protos.Volume.Mode
import scala.collection.JavaConversions._
import org.apache.mesos.Protos.{Offer, Resource, Value, Volume}
import ly.stealth.mesos.kafka.Broker.{ExecutionOptions, Failover, Metrics, Stickiness}
import ly.stealth.mesos.kafka.Util.BindAddress
import ly.stealth.mesos.kafka.json.JsonUtil
import ly.stealth.mesos.kafka.scheduler.mesos.OfferResult
import net.elodina.mesos.util.{Constraint, Period, Range, Repr}

class Broker(val id: Int = 0) {
  @volatile var active: Boolean = false

  var cpus: Double = 1
  var mem: Long = 2048
  var heap: Long = 1024
  var port: Range = null
  var volume: String = null
  var bindAddress: BindAddress = null
  var syslog: Boolean = false

  var constraints: Map[String, Constraint] = Map()
  var options: Map[String, String] = Map()
  var log4jOptions: Map[String, String] = Map()

  var stickiness: Stickiness = new Stickiness()
  var failover: Failover = new Failover()

  var metrics: Metrics = Metrics()

  // broker has been modified while being in non stopped state, once stopped or before task launch becomes false
  var needsRestart: Boolean = false

  var executionOptions: ExecutionOptions = ExecutionOptions()

  @volatile var task: Broker.Task = _
  @volatile var lastTask: Broker.Task = _

  def matches(offer: Offer, now: Date = new Date(), otherAttributes: Broker.OtherAttributes = Broker.NoAttributes): OfferResult = {
    // check resources
    val reservation = getReservation(offer)
    if (reservation.cpus < cpus) return OfferResult.neverMatch(offer, this, s"cpus < $cpus")
    if (reservation.mem < mem) return OfferResult.neverMatch(offer, this, s"mem < $mem")
    if (reservation.port == -1) return OfferResult.neverMatch(offer, this, "no suitable port")

    // check volume
    if (volume != null && reservation.volume == null)
      return OfferResult.neverMatch(offer, this, s"offer missing volume: $volume")

    // check attributes
    val offerAttributes = new util.HashMap[String, String]()
    offerAttributes.put("hostname", offer.getHostname)

    for (attribute <- offer.getAttributesList)
      if (attribute.hasText) offerAttributes.put(attribute.getName, attribute.getText.getValue)

    // check constraints
    for ((name, constraint) <- constraints) {
      if (!offerAttributes.containsKey(name)) return OfferResult.neverMatch(offer, this, s"no $name")
      if (!constraint.matches(offerAttributes.get(name), otherAttributes(name)))
        return OfferResult.neverMatch(offer, this, s"$name doesn't match $constraint")
    }

    // check stickiness
    val stickyTimeLeft = stickiness.stickyTimeLeft(now)
    if (stickyTimeLeft > 0)
      if (!stickiness.matchesHostname(offer.getHostname))
        return OfferResult.eventuallyMatch(offer, this, "hostname != stickiness host", stickyTimeLeft)

    // check failover delay
    val failoverDelay = (failover.delayExpires.getTime - now.getTime) / 1000
    if (failoverDelay > 0) {
      return OfferResult.eventuallyMatch(offer, this, "waiting to restart", failoverDelay.toInt)
    }

    // Accept it
    OfferResult.Accept(offer, this)
  }

  def getReservation(offer: Offer): Broker.Reservation = {
    var sharedCpus: Double = 0
    var roleCpus: Double = 0
    var reservedSharedCpus: Double = 0
    var reservedRoleCpus: Double = 0

    var sharedMem: Long = 0
    var roleMem: Long = 0
    var reservedSharedMem: Long = 0
    var reservedRoleMem: Long = 0

    val sharedPorts: util.List[Range] = new util.ArrayList[Range]()
    val rolePorts: util.List[Range] = new util.ArrayList[Range]()
    var reservedSharedPort: Long = -1
    var reservedRolePort: Long = -1

    var role: String = null

    var reservedVolume: String = null
    var reservedVolumeSize: Double = 0
    var reservedVolumePrincipal: String = null
    var reservedVolumeSource: Source = null

    for (resource <- offer.getResourcesList) {
      if (resource.getRole == "*") {
        // shared resources
        if (resource.getName == "cpus") sharedCpus = resource.getScalar.getValue
        if (resource.getName == "mem") sharedMem = resource.getScalar.getValue.toLong
        if (resource.getName == "ports") sharedPorts.addAll(resource.getRanges.getRangeList.map(r => new Range(r.getBegin.toInt, r.getEnd.toInt)))
      } else {
        if (role != null && role != resource.getRole)
          throw new IllegalArgumentException(s"Offer contains 2 non-default roles: $role, ${resource.getRole}")
        role = resource.getRole

        // static role-reserved resources
        if (!resource.hasReservation) {
          if (resource.getName == "cpus") roleCpus = resource.getScalar.getValue
          if (resource.getName == "mem") roleMem = resource.getScalar.getValue.toLong
          if (resource.getName == "ports") rolePorts.addAll(resource.getRanges.getRangeList.map(r => new Range(r.getBegin.toInt, r.getEnd.toInt)))
        }

        // dynamic role/principal-reserved volume
        if (volume != null && resource.hasDisk && resource.getDisk.hasPersistence && resource.getDisk.getPersistence.getId == volume) {
          reservedVolume = volume
          reservedVolumeSize = resource.getScalar.getValue
          reservedVolumePrincipal = resource.getReservation.getPrincipal
          // will be NULL for root volumes
          reservedVolumeSource = resource.getDisk.getSource
        }
      }
    }

    reservedRoleCpus = Math.min(cpus, roleCpus)
    reservedSharedCpus = Math.min(cpus - reservedRoleCpus, sharedCpus)

    reservedRoleMem = Math.min(mem, roleMem)
    reservedSharedMem = Math.min(mem - reservedRoleMem, sharedMem)

    reservedRolePort = getSuitablePort(rolePorts)
    if (reservedRolePort == -1)
      reservedSharedPort = getSuitablePort(sharedPorts)

    new Broker.Reservation(role,
      reservedSharedCpus, reservedRoleCpus,
      reservedSharedMem, reservedRoleMem,
      reservedSharedPort, reservedRolePort,
      reservedVolume, reservedVolumeSize,
      reservedVolumePrincipal, reservedVolumeSource
    )
  }

  private[kafka] def getSuitablePort(ports: util.List[Range]): Int = {
    if (ports.isEmpty) return -1

    val ports_ = ports.sortBy(r => r.start)
    if (port == null)
      return ports_.get(0).start

    for (range <- ports_) {
      val overlap = range.overlap(port)
      if (overlap != null)
        return overlap.start
    }

    -1
  }

  /*
  An "steady state" broker is a broker that is either running happily,
  or stopped and won't be started.
   */
  def isSteadyState: Boolean = {
    (active && task != null && task.running) || !active
  }

  def shouldStart(hostname: String, now: Date = new Date()): Boolean =
    active && task == null

  def shouldStop: Boolean = !active && task != null && !task.stopping

  def registerStart(hostname: String): Unit = {
    stickiness.registerStart(hostname)
    failover.resetFailures()
  }

  def registerStop(now: Date = new Date(), failed: Boolean = false): Unit = {
    if (!failed || failover.failures == 0) stickiness.registerStop(now)

    if (failed) failover.registerFailure(now)
    else failover.resetFailures()
  }

  def state(now: Date = new Date()): String = {
    if (task != null && !task.starting) return task.state

    if (active) {
      if (failover.isWaitingDelay(now)) {
        var s = "failed " + failover.failures
        if (failover.maxTries != null) s += "/" + failover.maxTries
        s += " " + Repr.dateTime(failover.failureTime)
        s += ", next start " + Repr.dateTime(failover.delayExpires)
        return s
      }

      if (failover.failures > 0) {
        var s = "starting " + (failover.failures + 1)
        if (failover.maxTries != null) s += "/" + failover.maxTries
        s += ", failed " + Repr.dateTime(failover.failureTime)
        return s
      }

      return "starting"
    }

    "stopped"
  }

  def waitFor(state: String, timeout: Period, minDelay: Int = 100): Boolean = {
    def matches: Boolean = if (state != null) task != null && task.state == state else task == null

    var t = timeout.ms
    while (t > 0 && !matches) {
      val delay = Math.min(minDelay, t)
      Thread.sleep(delay)
      t -= delay
    }

    matches
  }

  def clone(newId: Int): Broker = {
    val nb = new Broker(newId)
    nb.cpus = cpus
    nb.mem = mem
    nb.heap = heap
    nb.port = port
    nb.volume = volume
    nb.bindAddress = bindAddress
    nb.syslog = syslog
    nb.stickiness.period = stickiness.period
    nb.constraints = Map() ++ constraints
    nb.options = Map() ++ options
    nb.log4jOptions = Map() ++ log4jOptions
    nb.failover.delay = failover.delay
    nb.failover.maxDelay = failover.maxDelay
    nb.failover.maxTries = failover.maxTries
    nb.executionOptions = executionOptions.copy()

    nb
  }

  override def toString: String = {
    JsonUtil.toJson(this)
  }

}

object Broker {
  def nextTaskId(broker: Broker): String = Config.frameworkName + "-" + broker.id + "-" + UUID.randomUUID()

  def nextExecutorId(broker: Broker): String = Config.frameworkName + "-" + broker.id + "-" + UUID.randomUUID()

  def idFromTaskId(taskId: String): Int = taskId.dropRight(37).replace(Config.frameworkName + "-", "").toInt

  def idFromExecutorId(executorId: String): Int = idFromTaskId(executorId)

  def isOptionOverridable(name: String): Boolean = !Set("broker.id", "port", "zookeeper.connect").contains(name)

  class Stickiness(_period: Period = new Period("10m")) {
    var period: Period = _period
    @volatile var hostname: String = null
    @volatile var stopTime: Date = null

    def expires: Date = if (stopTime != null) new Date(stopTime.getTime + period.ms) else null

    def registerStart(hostname: String): Unit = {
      this.hostname = hostname
      stopTime = null
    }

    def registerStop(now: Date = new Date()): Unit = {
      this.stopTime = now
    }

    def stickyTimeLeft(now: Date = new Date()): Int =
      if (stopTime == null || hostname == null)
        0
      else
        (((stopTime.getTime - now.getTime) + period.ms) / 1000).toInt

    def allowsHostname(hostname: String, now: Date = new Date()): Boolean =
      stickyTimeLeft(now) <= 0 || matchesHostname(hostname)

    def matchesHostname(hostname: String) =
      this.hostname == null || this.hostname == hostname

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Stickiness]) return false
      val other = obj.asInstanceOf[Stickiness]
      period == other.period && stopTime == other.stopTime && hostname == other.hostname
    }
  }

  abstract class ContainerType
  object ContainerType {
    object Mesos extends ContainerType {
      override def toString: String = "mesos"
    }
    object Docker extends ContainerType {
      override def toString: String = "docker"
    }

    def valueOf(s: String): ContainerType = {
      s match {
        case "mesos" => ContainerType.Mesos
        case "docker" => ContainerType.Docker
      }
    }
  }

  case class Container(ctype: ContainerType, name: String, mounts: Seq[Mount] = Seq())

  abstract class MountMode
  object MountMode {
    object ReadWrite extends MountMode {
      override def toString: String = "rw"
    }
    object ReadOnly extends MountMode {
      override def toString: String = "ro"
    }
  }
  case class Mount(hostPath: String, containerPath: String, mode: MountMode)

  object Mount {
    def parse(v: String): Mount = {
      v.split(':') match {
        case Array(host, container) => Mount(host, container, MountMode.ReadWrite)
        case Array(host, container, mode) =>
          Mount(host, container, mode.toLowerCase() match {
            case "rw" => MountMode.ReadWrite
            case "r" | "ro" => MountMode.ReadOnly
            case _ => throw new IllegalArgumentException("invalid mount mode " + mode)
          })
        case _ => throw new IllegalArgumentException("invalid mount " + v)
      }
    }
  }
  case class ExecutionOptions(
    container: Option[Container] = None,
    jvmOptions: String = "",
    javaCmd: String = "exec java"
  )

  class Failover(_delay: Period = new Period("1m"), _maxDelay: Period = new Period("10m")) {
    var delay: Period = _delay
    var maxDelay: Period = _maxDelay
    var maxTries: Integer = null

    @volatile var failures: Int = 0
    @volatile var failureTime: Date = null

    def currentDelay: Period = {
      if (failures == 0) return new Period("0")

      val multiplier = 1 << Math.min(30, failures - 1)
      val d = delay.ms * multiplier

      if (d > maxDelay.ms) maxDelay else new Period(delay.value * multiplier + delay.unit)
    }

    def delayExpires: Date = {
      if (failures == 0) return new Date(0)
      new Date(failureTime.getTime + currentDelay.ms)
    }

    def isWaitingDelay(now: Date = new Date()): Boolean = delayExpires.getTime > now.getTime

    def isMaxTriesExceeded: Boolean = {
      if (maxTries == null) return false
      failures >= maxTries
    }

    def registerFailure(now: Date = new Date()): Unit = {
      failures += 1
      failureTime = now
    }

    def resetFailures(): Unit = {
      failures = 0
      failureTime = null
    }

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Failover])
        return false
      val other = obj.asInstanceOf[Failover]

      (delay == other.delay
        && maxDelay == other.maxDelay
        && maxTries == other.maxTries
        && failures == other.failures
        && failureTime == other.failureTime)
    }
  }

  case class Task(
    id: String = null,
    slaveId: String = null,
    executorId: String = null,
    hostname: String = null,
    attributes: Map[String, String] = Map()
  ) {
    @volatile var state: String = State.PENDING
    var endpoint: Endpoint = null

    def pending: Boolean = state == State.PENDING

    def starting: Boolean = state == State.STARTING

    def running: Boolean = state == State.RUNNING

    def stopping: Boolean = state == State.STOPPING

    def reconciling: Boolean = state == State.RECONCILING

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Task]) return false
      val other = obj.asInstanceOf[Task]
      (id == other.id
        && executorId == other.executorId
        && slaveId == other.slaveId
        && hostname == other.hostname
        && endpoint == other.endpoint
        && attributes == other.attributes)
    }
  }

  class Endpoint(s: String) {
    var hostname: String = null
    var port: Int = -1

    {
      val idx = s.indexOf(":")
      if (idx == -1) throw new IllegalArgumentException(s)

      hostname = s.substring(0, idx)
      port = Integer.parseInt(s.substring(idx + 1))
    }

    def this(hostname: String, port: Int) = this(hostname + ":" + port)

    override def equals(obj: scala.Any): Boolean = {
      if (!obj.isInstanceOf[Endpoint]) return false
      val endpoint = obj.asInstanceOf[Endpoint]
      hostname == endpoint.hostname && port == endpoint.port
    }

    override def hashCode(): Int = 31 * hostname.hashCode + port.hashCode()

    override def toString: String = hostname + ":" + port
  }

  class Reservation( val role: String = null,
                     val sharedCpus: Double = 0.0,
                     val roleCpus: Double = 0.0,
                     val sharedMem: Long = 0,
                     val roleMem: Long = 0,
                     val sharedPort: Long = -1,
                     val rolePort: Long = -1,
                     val volume: String = null,
                     val volumeSize: Double = 0.0,
                     val volumePrincipal: String = null,
                     val diskSource: Source = null
                     ) {

    def cpus: Double = sharedCpus + roleCpus
    val mem: Long = sharedMem + roleMem
    val port: Long = if (rolePort != -1) rolePort else sharedPort

    def toResources: util.List[Resource] = {
      def cpus(value: Double, role: String): Resource = {
        Resource.newBuilder
          .setName("cpus")
          .setType(Value.Type.SCALAR)
          .setScalar(Value.Scalar.newBuilder.setValue(value))
          .setRole(role)
          .build()
      }

      def mem(value: Long, role: String): Resource = {
        Resource.newBuilder
          .setName("mem")
          .setType(Value.Type.SCALAR)
          .setScalar(Value.Scalar.newBuilder.setValue(value))
          .setRole(role)
          .build()
      }

      def port(value: Long, role: String): Resource = {
        Resource.newBuilder
          .setName("ports")
          .setType(Value.Type.RANGES)
          .setRanges(Value.Ranges.newBuilder.addRange(Value.Range.newBuilder().setBegin(value).setEnd(value)))
          .setRole(role)
          .build()
      }

      def volumeDisk(id: String, value: Double, role: String, principal: String, diskSource: Source): Resource = {
        // TODO: add support for changing container path
        val volume = Volume.newBuilder.setMode(Mode.RW).setContainerPath("data").build()
        val persistence = Persistence.newBuilder.setId(id).build()

        val diskBuilder = DiskInfo.newBuilder
          .setPersistence(persistence)
          .setVolume(volume)
        if (diskSource != null && diskSource.hasType)
          diskBuilder.setSource(diskSource)

        val disk = diskBuilder.build()

        val reservation = ReservationInfo.newBuilder.setPrincipal(principal).build()
        Resource.newBuilder
          .setName("disk")
          .setType(Value.Type.SCALAR)
          .setScalar(Value.Scalar.newBuilder.setValue(value))
          .setRole(role)
          .setDisk(disk)
          .setReservation(reservation)
          .build()
      }

      val resources: util.List[Resource] = new util.ArrayList[Resource]()

      if (sharedCpus > 0) resources.add(cpus(sharedCpus, "*"))
      if (roleCpus > 0) resources.add(cpus(roleCpus, role))

      if (sharedMem > 0) resources.add(mem(sharedMem, "*"))
      if (roleMem > 0) resources.add(mem(roleMem, role))

      if (sharedPort != -1) resources.add(port(sharedPort, "*"))
      if (rolePort != -1) resources.add(port(rolePort, role))

      if (volume != null) resources.add(volumeDisk(volume, volumeSize, role, volumePrincipal, diskSource))
      resources
    }
  }

  case class Metrics(data: Map[String, Number] = Map(), timestamp: Long = 0) {
    def apply(metric: String) = data.get(metric)
  }

  object State {
    val PENDING = "pending"
    val STARTING = "starting"
    val RUNNING = "running"
    val RECONCILING = "reconciling"
    val STOPPING = "stopping"
  }

  type OtherAttributes = (String) => util.Collection[String]

  def NoAttributes: OtherAttributes = _ => Seq()
}
