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

import com.google.protobuf.Descriptors
import org.apache.mesos.Protos.Resource.DiskInfo
import org.apache.mesos.Protos.Resource.DiskInfo.{Persistence, Source}
import org.apache.mesos.Protos.Resource.DiskInfo.Source.{Mount, Type}
import org.junit.{Before, Test}
import org.junit.Assert._
import ly.stealth.mesos.kafka.Util.BindAddress
import net.elodina.mesos.util.{Period, Range, Constraint}
import net.elodina.mesos.util.Strings.parseMap
import java.util.{Collections, Date}
import scala.collection.JavaConversions._
import ly.stealth.mesos.kafka.Broker.{Endpoint, Stickiness, State, Task, Failover}
import java.util
import org.apache.mesos.Protos.{Value, Volume, Resource, Offer}

class BrokerTest extends KafkaMesosTestCase {
  var broker: Broker = _

  @Before
  override def before() {
    super.before()
    broker = new Broker("0")
    broker.cpus = 0
    broker.mem = 0
  }

  @Test
  def options() {
    // $id substitution
    broker.options = parseMap("a=$id,b=2")
    assertEquals(parseMap("a=0,b=2"), broker.options())

    // defaults
    broker.options = parseMap("a=2")
    assertEquals(parseMap("a=2,b=1"), broker.options(parseMap("a=2,b=1")))

    // bind-address
    broker.options = parseMap("host.name=123")
    assertEquals(parseMap("host.name=123"), broker.options())

    broker.bindAddress = new BindAddress("127.0.0.1")
    assertEquals(parseMap("host.name=127.0.0.1"), broker.options())
  }

  @Test
  def matches() {
    // cpus
    broker.cpus = 0.5
    assertNull(broker.matches(offer("cpus:0.2; cpus(role):0.3; ports:1000")))
    assertEquals("cpus < 0.5", broker.matches(offer("cpus:0.2; cpus(role):0.2")))
    broker.cpus = 0

    // mem
    broker.mem = 100
    assertNull(broker.matches(offer("mem:70; mem(role):30; ports:1000")))
    assertEquals("mem < 100", broker.matches(offer("mem:70; mem(role):29")))
    broker.mem = 0

    // port
    assertNull(broker.matches(offer("ports:1000")))
    assertEquals("no suitable port", broker.matches(offer("")))
  }

  @Test
  def matches_hostname() {
    val now = new Date(0)
    val resources: String = "ports:0..10"

    assertNull(broker.matches(offer("master", resources)))
    assertNull(broker.matches(offer("slave", resources)))

    // token
    broker.constraints = parseMap("hostname=like:master").mapValues(new Constraint(_))
    assertNull(broker.matches(offer("master", resources)))
    assertEquals("hostname doesn't match like:master", broker.matches(offer("slave", resources)))

    // like
    broker.constraints = parseMap("hostname=like:master.*").mapValues(new Constraint(_))
    assertNull(broker.matches(offer("master", resources)))
    assertNull(broker.matches(offer("master-2", resources)))
    assertEquals("hostname doesn't match like:master.*", broker.matches(offer("slave", resources)))

    // unique
    broker.constraints = parseMap("hostname=unique").mapValues(new Constraint(_))
    assertNull(broker.matches(offer("master", resources)))
    assertEquals("hostname doesn't match unique", broker.matches(offer("master", resources), now, _ => util.Arrays.asList("master")))
    assertNull(broker.matches(offer("master", resources), now, _ => util.Arrays.asList("slave")))

    // groupBy
    broker.constraints = parseMap("hostname=groupBy").mapValues(new Constraint(_))
    assertNull(broker.matches(offer("master", resources)))
    assertNull(broker.matches(offer("master", resources), now, _ => util.Arrays.asList("master")))
    assertEquals("hostname doesn't match groupBy", broker.matches(offer("master", resources), now, _ => util.Arrays.asList("slave")))
  }

  @Test
  def matches_stickiness() {
    val host0 = "host0"
    val host1 = "host1"
    val resources = "ports:0..10"

    assertNull(broker.matches(offer(host0, resources), new Date(0)))
    assertNull(broker.matches(offer(host1, resources), new Date(0)))

    broker.registerStart(host0)
    broker.registerStop(new Date(0))

    assertNull(broker.matches(offer(host0, resources), new Date(0)))
    assertEquals("hostname != stickiness host", broker.matches(offer(host1, resources), new Date(0)))
  }

  @Test
  def matches_attributes() {
    val now = new Date(0)

    def offer(attributes: String): Offer = this.offer("id", "fw-id", "slave-id", "host", "ports:0..10", attributes)

    // like
    broker.constraints = parseMap("rack=like:1-.*").mapValues(new Constraint(_))
    assertNull(broker.matches(offer("rack=1-1")))
    assertNull(broker.matches(offer("rack=1-2")))
    assertEquals("rack doesn't match like:1-.*", broker.matches(offer("rack=2-1")))

    // groupBy
    broker.constraints = parseMap("rack=groupBy").mapValues(new Constraint(_))
    assertNull(broker.matches(offer("rack=1")))
    assertNull(broker.matches(offer("rack=1"), now, _ => util.Arrays.asList("1")))
    assertEquals("rack doesn't match groupBy", broker.matches(offer("rack=2"), now, _ => util.Arrays.asList("1")))
  }

  @Test
  def get_reservations_dynamic() = {
    broker.cpus = 2
    broker.mem = 200
    // ignore non-dynamically reserved disk
    var reservation = broker.getReservation(offer("cpus:2; mem:100; ports:1000; disk:1000"))
    assertEquals(resources("cpus:2; mem:100; ports:1000"), reservation.toResources)

    // Ignore resources with a principal
    reservation = broker.getReservation(offer("cpus:2; mem:100; ports:1000; mem(*,principal):100"))
    assertEquals(resources("cpus:2; mem:100; ports:1000"), reservation.toResources)

    // Ignore resources with a principal + role
    reservation = broker.getReservation(offer("cpus:2; mem:100; ports:1000; mem(role,principal):100"))
    assertEquals(resources("cpus:2; mem:100; ports:1000"), reservation.toResources)

    // pay attention resources with a role
    reservation = broker.getReservation(offer("cpus:2; mem:100; ports:1000; mem(role):100"))
    assertEquals(resources("cpus:2; mem:100; mem(role):100; ports:1000"), reservation.toResources)
  }

  @Test
  def get_reservations_volume() = {
    broker.cpus = 2
    broker.mem = 200
    broker.volume = "test"

    val reservation = broker.getReservation(offer("cpus:2; mem:100; ports:1000; disk(role,principal)[test:mount_point]:100"))
    val resource = resources("cpus:2; mem:100; ports:1000; disk(role,principal)[test:data]:100")
    assertEquals(resource, reservation.toResources)
  }

  def diskResourceWithSource(disk: String, `type`: Source.Type, path: String): Resource = {
    val withoutSource = resources(disk).get(0)
    val sourceBuilder = Source.newBuilder()
    sourceBuilder.setType(`type`)
    if (`type` == Source.Type.MOUNT) {
      sourceBuilder.setMount(Source.Mount.newBuilder().setRoot(path))
    } else {
      sourceBuilder.setPath(Source.Path.newBuilder().setRoot(path))
    }

    withoutSource.
      toBuilder.
      setDisk(
        withoutSource.
          getDisk.
          toBuilder.
          setSource(sourceBuilder)).
      build
  }
  @Test
  def getReservations_volumeMount(): Unit = {
    broker.cpus = 2
    broker.cpus = 2
    broker.mem = 200
    broker.volume = "test"

    val persistentVolumeResource = diskResourceWithSource(
      "disk(role,principal)[test:mount_point]:100",
      Source.Type.MOUNT,
      "/mnt/path")


    val thisOffer = offer("").toBuilder().
      addAllResources(resources("cpus:2; mem:100; ports:1000")).
      addResources(persistentVolumeResource).
      build()

    val reservation = broker.getReservation(thisOffer)

    assertEquals(reservation.diskSource, persistentVolumeResource.getDisk().getSource())
  }

  @Test
  def get_reservations() {
    broker.cpus = 2
    broker.mem = 100

    // shared resources
    var reservation = broker.getReservation(offer("cpus:3; mem:200; ports:1000..2000"))
    assertEquals(resources("cpus:2; mem:100; ports:1000"), reservation.toResources)

    // role resources
    reservation = broker.getReservation(offer("cpus(role):3; mem(role):200; ports(role):1000..2000"))
    assertEquals(resources("cpus(role):2; mem(role):100; ports(role):1000"), reservation.toResources)

    // mixed resources
    reservation = broker.getReservation(offer("cpus:2; cpus(role):1; mem:100; mem(role):99; ports:1000..2000; ports(role):3000"))
    assertEquals(resources("cpus:1; cpus(role):1; mem:1; mem(role):99; ports(role):3000"), reservation.toResources)

    // not enough resources
    reservation = broker.getReservation(offer("cpus:0.5; cpus(role):0.5; mem:1; mem(role):1; ports:1000"))
    assertEquals(resources("cpus:0.5; cpus(role):0.5; mem:1; mem(role):1; ports:1000"), reservation.toResources)

    // no port
    reservation = broker.getReservation(offer(""))
    assertEquals(-1, reservation.port)

    // two non-default roles
    try {
      broker.getReservation(offer("cpus(r1):0.5; mem(r2):100"))
      fail()
    } catch {
      case e: IllegalArgumentException =>
        val m: String = e.getMessage
        assertTrue(m, m.contains("r1") && m.contains("r2"))
    }
  }


  @Test
  def get_suitablePort() {
    def ranges(s: String): util.List[Range] = {
      if (s.isEmpty) return Collections.emptyList()
      s.split(",").toList.map(s => new Range(s.trim))
    }

    // no port restrictions
    assertEquals(-1, broker.getSuitablePort(ranges("")))
    assertEquals(100, broker.getSuitablePort(ranges("100..100")))
    assertEquals(100, broker.getSuitablePort(ranges("100..200")))

    // order
    assertEquals(10, broker.getSuitablePort(ranges("30,10,20,40")))
    assertEquals(50, broker.getSuitablePort(ranges("100..200, 50..60")))

    // single port restriction
    broker.port = new Range(92)
    assertEquals(-1, broker.getSuitablePort(ranges("0..91")))
    assertEquals(-1, broker.getSuitablePort(ranges("93..100")))
    assertEquals(92, broker.getSuitablePort(ranges("90..100")))

    // port range restriction
    broker.port = new Range("92..100")
    assertEquals(-1, broker.getSuitablePort(ranges("0..91")))
    assertEquals(-1, broker.getSuitablePort(ranges("101..200")))
    assertEquals(92, broker.getSuitablePort(ranges("0..100")))
    assertEquals(92, broker.getSuitablePort(ranges("0..92")))

    assertEquals(100, broker.getSuitablePort(ranges("100..200")))
    assertEquals(95, broker.getSuitablePort(ranges("0..90,95..96,101..200")))
    assertEquals(96, broker.getSuitablePort(ranges("0..90,96,101..200")))
  }

  @Test
  def shouldStart() {
    val host = "host"
    // active
    broker.active = false
    assertFalse(broker.shouldStart(host))
    broker.active = true
    assertTrue(broker.shouldStart(host))

    // has task
    broker.task = new Task()
    assertFalse(broker.shouldStart(host))
    broker.task = null
    assertTrue(broker.shouldStart(host))

    // failover waiting delay
    val now = new Date(0)
    broker.failover.delay = new Period("1s")
    broker.registerStop(now, failed = true)
    assertTrue(broker.failover.isWaitingDelay(now))

    assertFalse(broker.shouldStart(host, now = now))
    assertTrue(broker.shouldStart(host, now = new Date(now.getTime + broker.failover.delay.ms)))
    broker.failover.resetFailures()
    assertTrue(broker.shouldStart(host, now = now))
  }

  @Test
  def shouldStop() {
    assertFalse(broker.shouldStop)

    broker.task = new Broker.Task()
    assertTrue(broker.shouldStop)

    broker.active = true
    assertFalse(broker.shouldStop)
  }

  @Test
  def state() {
    assertEquals("stopped", broker.state())

    broker.task = new Task(_state = State.STOPPING)
    assertEquals("stopping", broker.state())

    broker.task = null
    broker.active = true
    assertEquals("starting", broker.state())

    broker.task = new Task()
    assertEquals("starting", broker.state())

    broker.task.state = State.RUNNING
    assertEquals("running", broker.state())

    broker.task = null
    broker.failover.delay = new Period("1s")
    broker.failover.registerFailure(new Date(0))
    var state = broker.state(new Date(0))
    assertTrue(state, state.startsWith("failed 1"))

    state = broker.state(new Date(1000))
    assertTrue(state, state.startsWith("starting 2"))
  }

  @Test(timeout = 5000)
  def waitFor() {
    def deferStateSwitch(state: String, delay: Long) {
      new Thread() {
        override def run() {
          setName(classOf[BrokerTest].getSimpleName + "-scheduleState")
          Thread.sleep(delay)

          if (state != null) broker.task = new Task(_state = state)
          else broker.task = null
        }
      }.start()
    }

    deferStateSwitch(Broker.State.RUNNING, 100)
    assertTrue(broker.waitFor(State.RUNNING, new Period("200ms")))

    deferStateSwitch(null, 100)
    assertTrue(broker.waitFor(null, new Period("200ms")))

    // timeout
    assertFalse(broker.waitFor(State.RUNNING, new Period("50ms")))
  }

  @Test
  def to_json_fromJson() {
    broker.active = true
    broker.cpus = 0.5
    broker.mem = 128
    broker.heap = 128
    broker.port = new Range("0..100")
    broker.volume = "volume"
    broker.bindAddress = new Util.BindAddress("192.168.0.1")
    broker.syslog = true

    broker.constraints = parseMap("a=like:1").mapValues(new Constraint(_))
    broker.options = parseMap("a=1")
    broker.log4jOptions = parseMap("b=2")
    broker.jvmOptions = "-Xms512m"

    broker.failover.registerFailure(new Date())
    broker.task = new Task("1", "slave", "executor", "host")

    val read: Broker = new Broker()
    read.fromJson(Util.parseJson("" + broker.toJson()))

    BrokerTest.assertBrokerEquals(broker, read)
  }

  // static part
  @Test
  def idFromTaskId() {
    assertEquals("0", Broker.idFromTaskId(Broker.nextTaskId(new Broker("0"))))
    assertEquals("100", Broker.idFromTaskId(Broker.nextTaskId(new Broker("100"))))
  }

  // Reservation
  @Test
  def Reservation_toResources() {
    // shared
    var reservation = new Broker.Reservation(null, sharedCpus = 0.5, sharedMem = 100, sharedPort = 1000)
    assertEquals(resources("cpus:0.5; mem:100; ports:1000"), reservation.toResources)

    // role
    reservation = new Broker.Reservation("role", roleCpus = 0.5, roleMem = 100, rolePort = 1000)
    assertEquals(resources("cpus(role):0.5; mem(role):100; ports(role):1000"), reservation.toResources)

    // shared + role
    reservation = new Broker.Reservation("role", sharedCpus = 0.3, roleCpus = 0.7, sharedMem = 50, roleMem = 100, sharedPort = 1000, rolePort = 2000)
    assertEquals(resources("cpus:0.3; cpus(role):0.7; mem:50; mem(role):100; ports:1000; ports(role):2000"), reservation.toResources)
  }

  // Stickiness
  @Test
  def Stickiness_allowsHostname() {
    val stickiness = new Stickiness()
    assertTrue(stickiness.allowsHostname("host0", new Date(0)))
    assertTrue(stickiness.allowsHostname("host1", new Date(0)))

    stickiness.registerStart("host0")
    stickiness.registerStop(new Date(0))
    assertTrue(stickiness.allowsHostname("host0", new Date(0)))
    assertFalse(stickiness.allowsHostname("host1", new Date(0)))
    assertTrue(stickiness.allowsHostname("host1", new Date(stickiness.period.ms)))
  }

  @Test
  def Stickiness_registerStart_registerStop() {
    val stickiness = new Stickiness()
    assertNull(stickiness.hostname)
    assertNull(stickiness.stopTime)

    stickiness.registerStart("host")
    assertEquals("host", stickiness.hostname)
    assertNull(stickiness.stopTime)

    stickiness.registerStop(new Date(0))
    assertEquals("host", stickiness.hostname)
    assertEquals(new Date(0), stickiness.stopTime)

    stickiness.registerStart("host1")
    assertEquals("host1", stickiness.hostname)
    assertNull(stickiness.stopTime)
  }

  @Test
  def Stickiness_toJson_fromJson() {
    val stickiness = new Stickiness()
    stickiness.registerStart("localhost")
    stickiness.registerStop(new Date(0))

    val read: Stickiness = new Stickiness()
    read.fromJson(Util.parseJson("" + stickiness.toJson))

    BrokerTest.assertStickinessEquals(stickiness, read)
  }


  // Failover
  @Test
  def Failover_currentDelay() {
    val failover = new Failover(new Period("1s"), new Period("5s"))

    failover.failures = 0
    assertEquals(new Period("0s"), failover.currentDelay)

    failover.failures = 1
    assertEquals(new Period("1s"), failover.currentDelay)

    failover.failures = 2
    assertEquals(new Period("2s"), failover.currentDelay)

    failover.failures = 3
    assertEquals(new Period("4s"), failover.currentDelay)

    failover.failures = 4
    assertEquals(new Period("5s"), failover.currentDelay)

    failover.failures = 32
    assertEquals(new Period("5s"), failover.currentDelay)

    failover.failures = 33
    assertEquals(new Period("5s"), failover.currentDelay)

    failover.failures = 100
    assertEquals(new Period("5s"), failover.currentDelay)

    // multiplier boundary
    failover.maxDelay = new Period(Integer.MAX_VALUE + "s")

    failover.failures = 30
    assertEquals(new Period((1 << 29) + "s"), failover.currentDelay)

    failover.failures = 31
    assertEquals(new Period((1 << 30) + "s"), failover.currentDelay)

    failover.failures = 32
    assertEquals(new Period((1 << 30) + "s"), failover.currentDelay)

    failover.failures = 100
    assertEquals(new Period((1 << 30) + "s"), failover.currentDelay)
  }

  @Test
  def Failover_delayExpires() {
    val failover = new Failover(new Period("1s"))
    assertEquals(new Date(0), failover.delayExpires)

    failover.registerFailure(new Date(0))
    assertEquals(new Date(1000), failover.delayExpires)

    failover.failureTime = new Date(1000)
    assertEquals(new Date(2000), failover.delayExpires)
  }

  @Test
  def Failover_isWaitingDelay() {
    val failover = new Failover(new Period("1s"))
    assertFalse(failover.isWaitingDelay(new Date(0)))

    failover.registerFailure(new Date(0))

    assertTrue(failover.isWaitingDelay(new Date(0)))
    assertTrue(failover.isWaitingDelay(new Date(500)))
    assertTrue(failover.isWaitingDelay(new Date(999)))
    assertFalse(failover.isWaitingDelay(new Date(1000)))
  }

  @Test
  def Failover_isMaxTriesExceeded() {
    val failover = new Failover()

    failover.failures = 100
    assertFalse(failover.isMaxTriesExceeded)

    failover.maxTries = 50
    assertTrue(failover.isMaxTriesExceeded)
  }

  @Test
  def Failover_registerFailure_resetFailures() {
    val failover = new Failover()
    assertEquals(0, failover.failures)
    assertNull(failover.failureTime)

    failover.registerFailure(new Date(1))
    assertEquals(1, failover.failures)
    assertEquals(new Date(1), failover.failureTime)

    failover.registerFailure(new Date(2))
    assertEquals(2, failover.failures)
    assertEquals(new Date(2), failover.failureTime)

    failover.resetFailures()
    assertEquals(0, failover.failures)
    assertNull(failover.failureTime)

    failover.registerFailure()
    assertEquals(1, failover.failures)
  }

  @Test
  def Failover_toJson_fromJson() {
    val failover = new Failover(new Period("1s"), new Period("5s"))
    failover.maxTries = 10
    failover.resetFailures()
    failover.registerFailure(new Date(0))

    val read: Failover = new Failover()
    read.fromJson(Util.parseJson("" + failover.toJson))

    BrokerTest.assertFailoverEquals(failover, read)
  }

  // Task
  @Test
  def Task_toJson_fromJson() {
    val task = new Task("id", "slave", "executor", "host", parseMap("a=1,b=2"), State.RUNNING)
    task.endpoint = new Endpoint("localhost:9092")

    val read: Task = new Task()
    read.fromJson(Util.parseJson("" + task.toJson))

    BrokerTest.assertTaskEquals(task, read)
  }
}

object BrokerTest {
  def assertBrokerEquals(expected: Broker, actual: Broker) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.id, actual.id)
    assertEquals(expected.active, actual.active)

    assertEquals(expected.cpus, actual.cpus, 0.001)
    assertEquals(expected.mem, actual.mem)
    assertEquals(expected.heap, actual.heap)
    assertEquals(expected.port, actual.port)
    assertEquals(expected.volume, actual.volume)
    assertEquals(expected.bindAddress, actual.bindAddress)
    assertEquals(expected.syslog, actual.syslog)

    assertEquals(expected.constraints, actual.constraints)
    assertEquals(expected.options, actual.options)
    assertEquals(expected.log4jOptions, actual.log4jOptions)
    assertEquals(expected.jvmOptions, actual.jvmOptions)

    assertFailoverEquals(expected.failover, actual.failover)
    assertTaskEquals(expected.task, actual.task)

    assertEquals(expected.needsRestart, actual.needsRestart)
  }

  def assertFailoverEquals(expected: Failover, actual: Failover) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.delay, actual.delay)
    assertEquals(expected.maxDelay, actual.maxDelay)
    assertEquals(expected.maxTries, actual.maxTries)

    assertEquals(expected.failures, actual.failures)
    assertEquals(expected.failureTime, actual.failureTime)
  }

  def assertStickinessEquals(expected: Stickiness, actual: Stickiness) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.period, actual.period)
    assertEquals(expected.stopTime, actual.stopTime)
    assertEquals(expected.hostname, actual.hostname)
  }

  def assertTaskEquals(expected: Task, actual: Task) {
    if (checkNulls(expected, actual)) return

    assertEquals(expected.id, actual.id)
    assertEquals(expected.executorId, actual.executorId)
    assertEquals(expected.slaveId, actual.slaveId)

    assertEquals(expected.hostname, actual.hostname)
    assertEquals(expected.endpoint, actual.endpoint)
    assertEquals(expected.attributes, actual.attributes)

    assertEquals(expected.state, actual.state)
  }

  private def checkNulls(expected: Object, actual: Object): Boolean = {
    if (expected == actual) return true
    if (expected == null) throw new AssertionError("actual != null")
    if (actual == null) throw new AssertionError("actual == null")
    false
  }
}
