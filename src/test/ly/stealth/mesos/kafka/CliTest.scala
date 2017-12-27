/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package ly.stealth.mesos.kafka

import org.apache.mesos.Protos.{ExecutorID, SlaveID, TaskState}
import org.junit.{After, Before, Test}
import org.junit.Assert._
import java.util
import scala.collection.JavaConversions._
import java.io.{ByteArrayOutputStream, PrintStream}
import ly.stealth.mesos.kafka.Broker.{Container, ContainerType, Mount, MountMode}
import ly.stealth.mesos.kafka.cli.Cli
import ly.stealth.mesos.kafka.json.JsonUtil
import ly.stealth.mesos.kafka.scheduler.Rebalancer
import net.elodina.mesos.util.{Period, Strings}

class CliTest extends KafkaMesosTestCase {
  val out: ByteArrayOutputStream = new ByteArrayOutputStream()

  @Before
  override def before {
    super.before

    startHttpServer()
    Cli.api = Config.api
    Cli.out = new PrintStream(out, true)

    startZkServer()
  }

  @After
  override def after {
    Cli.out = System.out
    stopHttpServer()
    super.after
    stopZkServer()
  }

  @Test
  def help {
    exec("help")
    assertOutContains("Usage:")
    assertOutContains("scheduler")
    assertOutContains("broker")
    assertOutContains("topic")

    // command help
    for (command <- "help scheduler broker topic".split(" ")) {
      exec("help " + command)
      assertOutContains("Usage: " + command)
    }
  }

  @Test
  def broker_list{
    registry.cluster.addBroker(new Broker(0))
    registry.cluster.addBroker(new Broker(1))
    registry.cluster.addBroker(new Broker(2))

    exec("broker list")
    assertOutContains("brokers:")
    assertOutContains("id: 0")
    assertOutContains("id: 1")
    assertOutContains("id: 2")

    // when broker needs restart
    val broker = registry.cluster.getBroker(0)
    broker.needsRestart = true
    exec("broker list")
    assertOutContains("(modified, needs restart)")
  }

  @Test
  def broker_add {
    exec("broker add 0 --cpus=0.1 --mem=128")
    assertOutContains("broker added:")
    assertOutContains("id: 0")
    assertOutContains("cpus:0.10, mem:128")

    assertEquals(1, registry.cluster.getBrokers.size())
    val broker = registry.cluster.getBroker(0)
    assertEquals(0.1, broker.cpus, 0.001)
    assertEquals(128, broker.mem)
  }

  @Test
  def broker_add_docker_image: Unit = {
    exec("broker add 0 --cpus=1 --mem=128 --container-image=test --java-cmd=/usr/bin/java")

    assertEquals(1, registry.cluster.getBrokers.size())
    val broker = registry.cluster.getBroker(0)
    assertEquals(
      Some(Container(
        ctype = ContainerType.Docker,
        name = "test"
      )), broker.executionOptions.container)
    assertEquals("/usr/bin/java", broker.executionOptions.javaCmd)
  }

  @Test
  def broker_add_mesos_image: Unit = {
    exec("broker add 0 --cpus=1 --mem=128 --container-image=test --container-type=mesos --java-cmd=/usr/bin/java")

    assertEquals(1, registry.cluster.getBrokers.size())
    val broker = registry.cluster.getBroker(0)
    assertEquals(
      Some(Container(
        ctype = ContainerType.Mesos,
        name = "test"
      )), broker.executionOptions.container)
    assertEquals("/usr/bin/java", broker.executionOptions.javaCmd)
  }

  @Test
  def broker_add_volume: Unit = {
    exec("broker add 0 --cpus=1 --mem=128 --container-image=test " +
      "--container-type=mesos " +
      "--container-mounts=/a:/b:ro " +
      "--java-cmd=/usr/bin/java")

    assertEquals(1, registry.cluster.getBrokers.size())
    val broker = registry.cluster.getBroker(0)
    assertEquals(
      Some(Container(
        ctype = ContainerType.Mesos,
        name = "test",
        mounts = Seq(Mount("/a", "/b", MountMode.ReadOnly))
      )), broker.executionOptions.container)
    assertEquals("/usr/bin/java", broker.executionOptions.javaCmd)
  }

  @Test
  def broker_update {
    val broker = registry.cluster.addBroker(new Broker(0))

    exec("broker update 0 --failover-delay=10s --failover-max-delay=20s --options=log.dirs=/tmp/kafka-logs")
    assertOutContains("broker updated:")
    assertOutContains("delay:10s, max-delay:20s")
    assertOutContains("options: log.dirs=/tmp/kafka-logs")

    assertEquals(new Period("10s"), broker.failover.delay)
    assertEquals(new Period("20s"), broker.failover.maxDelay)
    assertEquals(Strings.parseMap("log.dirs=/tmp/kafka-logs").toMap, broker.options)
  }

  @Test
  def broker_remove {
    registry.cluster.addBroker(new Broker(0))
    exec("broker remove 0")

    assertOutContains("broker 0 removed")
    assertNull(registry.cluster.getBroker(0))
  }

  @Test
  def broker_start_stop {
    val broker0 = registry.cluster.addBroker(new Broker(0))
    val broker1 = registry.cluster.addBroker(new Broker(1))

    exec("broker start * --timeout=0")
    assertOutContains("brokers scheduled to start:")
    assertOutContains("id: 0")
    assertOutContains("id: 1")
    assertTrue(broker0.active)
    assertTrue(broker1.active)

    exec("broker stop 0 --timeout=0")
    assertOutContains("broker scheduled to stop:")
    assertOutContains("id: 0")
    assertFalse(broker0.active)
    assertTrue(broker1.active)

    exec("broker stop 1 --timeout=0")
    assertOutContains("broker scheduled to stop:")
    assertOutContains("id: 1")
    assertFalse(broker0.active)
    assertFalse(broker1.active)
  }

  @Test
  def broker_start_stop_timeout {
    val broker = registry.cluster.addBroker(new Broker(0))
    try { exec("broker start 0 --timeout=1ms"); fail() }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("broker start timeout")) }
    assertTrue(broker.active)

    broker.task = Broker.Task("id", "slave", "executor", "host")
    broker.task.state = Broker.State.RUNNING
    try { exec("broker stop 0 --timeout=1ms"); fail() }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("broker stop timeout")) }
    assertFalse(broker.active)
  }

  @Test
  def broker_clone: Unit = {
    val broker = new Broker(0)
    broker.cpus = 5
    broker.options = Map("test" -> "abc")
    registry.cluster.addBroker(broker)
    exec("broker clone 1 --source 0")

    val newBroker = registry.cluster.getBroker(1)
    assertNotNull(newBroker)
    assertEquals(1, newBroker.id)
    assertEquals(newBroker.cpus, 5, 0)
  }

  @Test(timeout = 60000)
  def broker_log: Unit = {
    // no broker
    assertCliErrorContains("broker log 0", "broker 0 not found")

    // broker isn't active or running
    val broker = registry.cluster.addBroker(new Broker(0))
    assertCliErrorContains("broker log 0", "broker 0 is not active")

    broker.active = true
    assertCliErrorContains("broker log 0", "broker 0 is not running")

    // not running when task is null
    assertCliErrorContains("broker log 0", "broker 0 is not running")

    import Broker.State._

    broker.task = new Broker.Task("id", "slave", "executor", "host")
    for(state <- Seq(STARTING, RUNNING, RECONCILING, STOPPING) if state != RUNNING) {
      broker.task.state = state
      assertCliErrorContains("broker log 0", "broker 0 is not running")
    }

    def setLogContent(content: String, delay: Period = new Period("100ms")) =
      new Thread {
        override def run(): Unit = {
          Thread.sleep(delay.ms)
          val lastData = schedulerDriver.sentFrameworkMessages.last.data
          val logRequest = LogRequest.parse(new String(lastData))
          registry.brokerLogManager.putLog(logRequest.requestId, content)
        }
      }.start()

    setLogContent("something")
    // retrieve log only for active and running broker
    broker.task.state = RUNNING
    try { exec("broker log 0 --timeout 1s") }
    catch { case e: Cli.Error => fail("") }

    assertOutContains("something")

    // with name
    setLogContent("something with name")
    exec("broker log 0 --name server.log --timeout 1s")
    assertOutContains("something with name")

    // with lines
    setLogContent("something with lines")
    exec("broker log 0 --lines 200 --timeout 1s")
    assertOutContains("something with lines")

    // with name, lines
    setLogContent("something with name with lines with timeout")
    exec("broker log 0 --name controller.log --lines 300 --timeout 1s")
    assertOutContains("something with name with lines with timeout")

    // timed out
    assertCliErrorContains("broker log 0 --timeout 1s", "broker 0 log retrieve timeout")
  }

  @Test
  def broker_restart_timeout: Unit = {
    exec("help broker")
    assertOutContains("restart    - restart broker")

    exec("help broker restart")
    assertOutContains("Restart broker")
    assertOutContains("Usage: broker restart <broker-expr> [options]")
    assertOutContains("--timeout")

    val broker0 = registry.cluster.addBroker(new Broker(0))
    val broker1 = registry.cluster.addBroker(new Broker(1))

    for (broker <- registry.cluster.getBrokers) {
      exec("broker start " + broker.id + " --timeout 0s")
      started(broker)
    }

    // timeout
    assertCliErrorContains("broker restart * --timeout 200ms", "broker 0 timeout on stop")
  }

  @Test
  def broker_restart_success = {
    val broker0 = registry.cluster.addBroker(new Broker(0))
    val broker1 = registry.cluster.addBroker(new Broker(1))

    for (broker <- registry.cluster.getBrokers) {
      exec("broker start " + broker.id + " --timeout 0s")
      started(broker)
    }

    delay("150ms") { stopped(broker0) }
    delay("250ms") { started(broker0) }
    delay("450ms") { stopped(broker1) }
    delay("650ms") { started(broker1) }
    exec("broker restart 0..1 --timeout 1s")
    assertOutContains("brokers restarted:")
    assertOutContains("id: 0")
    assertOutContains("id: 1")
  }

  @Test
  def topic_list {
    exec("topic list")
    assertOutContains("no topics")

    registry.cluster.topics.addTopic("t0")
    registry.cluster.topics.addTopic("t1")
    registry.cluster.topics.addTopic("x")

    // list all
    exec("topic list")
    assertOutContains("topics:")
    assertOutContains("t0")
    assertOutContains("t1")
    assertOutContains("x")

    // name filtering
    exec("topic list t*")
    assertOutContains("t0")
    assertOutContains("t1")
    assertOutNotContains("x")
  }

  @Test
  def topic_delete {
    registry.cluster.topics.addTopic("t0")
    registry.cluster.topics.addTopic("t1")
    registry.cluster.topics.addTopic("x")
    registry.cluster.topics.addTopic("z")

    // delete single topic
    exec("topic delete z")
    assertOutContains("topic deleted:")
    assertOutContains("z")
    assertOutNotContains("t0")
    assertOutNotContains("t1")
    assertOutNotContains("x")

    // delete pattern
    exec("topic delete t*")
    assertOutContains("topics deleted:")
    assertOutContains("t0")
    assertOutContains("t1")
    assertOutNotContains("x")

    // delete is done asynchronously
    delay("500ms") {
      // delete all
      exec("topic delete *")
      assertOutContains("topic deleted:")
      assertOutContains("x")
    }
  }

  @Test
  def topic_add {
    exec("topic add t0")
    assertOutContains("topic added:")
    assertOutContains("name: t0")

    exec("topic list")
    assertOutContains("topic:")
    assertOutContains("name: t0")
    assertOutContains("partitions: 0:[0]")

    exec("topic add t1 --partition 2")
    exec("topic list t1")
    assertOutContains("topic:")
    assertOutContains("name: t1")
    assertOutContains("partitions: 0:[0], 1:[0]")
  }

  @Test
  def topic_partitions {
    exec("topic partitions t0")
    assertOutContains("topic not found")

    registry.cluster.topics.addTopic("t0")
    exec("topic partitions t0")
    assertOutContains("t0:")
    assertOutContains("[*0]")
  }

  @Test
  def topic_update {
    registry.cluster.topics.addTopic("t0")
    exec("topic update t0 --options=flush.ms=5000")
    assertOutContains("topic updated:")
    assertOutContains("name: t0")

    exec("topic list")
    assertOutContains("topic:")
    assertOutContains("t0")
    assertOutContains("flush.ms=5000")
  }

  @Test
  def topic_rebalance {
    val cluster: Cluster = registry.cluster
    val rebalancer: Rebalancer = cluster.rebalancer

    cluster.addBroker(new Broker(0))
    cluster.addBroker(new Broker(1))
    assertFalse(rebalancer.running)

    cluster.topics.addTopic("t")
    exec("topic rebalance *")
    assertTrue(rebalancer.running)
    assertOutContains("Rebalance started")
  }

  @Test
  def usage_errors {
    // no command
    try { exec(""); fail() }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("command required")) }

    // no id
    try { exec("broker add"); fail()  }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("argument required")) }

    // invalid command
    try { exec("unsupported 0"); fail()  }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("unsupported command")) }
  }

  @Test
  def connection_refused {
    registry.httpServer.stop()
    try {
      try { exec("broker add 0"); fail()  }
      catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains("Connection refused")) }
    } finally {
      registry.httpServer.start()
    }
  }

  private def assertOutContains(s: String): Unit = assertTrue("" + out, out.toString.contains(s))
  private def assertOutNotContains(s: String): Unit = assertFalse("" + out, out.toString.contains(s))

  private def exec(cmd: String): Unit = {
    out.reset()

    val args = new util.ArrayList[String]()
    for (arg <- cmd.split(" "))
      if (!cmd.isEmpty) args.add(arg)
    Cli.exec(args.toArray(new Array[String](args.length)))
  }

  private def assertCliErrorContains(cmd: String, str: String) =
    try { exec(cmd); fail() }
    catch { case e: Cli.Error => assertTrue(e.getMessage, e.getMessage.contains(str)) }
}
