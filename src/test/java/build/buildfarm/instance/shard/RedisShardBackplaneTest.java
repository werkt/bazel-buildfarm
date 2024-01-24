// Copyright 2018 The Bazel Authors. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package build.buildfarm.instance.shard;

import static build.buildfarm.instance.shard.RedisShardBackplane.parseOperationChange;
import static com.google.common.truth.Truth.assertThat;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static redis.clients.jedis.args.ListDirection.LEFT;
import static redis.clients.jedis.args.ListDirection.RIGHT;

import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.config.Queue;
import build.buildfarm.v1test.DispatchedOperation;
import build.buildfarm.v1test.ExecuteEntry;
import build.buildfarm.v1test.OperationChange;
import build.buildfarm.v1test.QueueEntry;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.v1test.WorkerChange;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.longrunning.Operation;
import com.google.protobuf.util.JsonFormat;
import java.io.IOException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.function.Supplier;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;
import redis.clients.jedis.UnifiedJedis;

@RunWith(JUnit4.class)
public class RedisShardBackplaneTest {
  private BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  @Mock Supplier<UnifiedJedis> mockJedisClusterFactory;

  @Before
  public void setUp() throws IOException {
    configs.getBackplane().setOperationExpire(10);
    configs.getBackplane().setQueues(new Queue[] {});
    MockitoAnnotations.initMocks(this);
  }

  public RedisShardBackplane createBackplane(String name) {
    return new RedisShardBackplane(
        name,
        /* subscribeToBackplane=*/ false,
        /* runFailsafeOperation=*/ false,
        o -> o,
        o -> o,
        mockJedisClusterFactory);
  }

  @Test
  public void workersWithInvalidProtobufAreRemoved() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    when(jedis.hgetAll(configs.getBackplane().getWorkersHashName() + "_storage"))
        .thenReturn(ImmutableMap.of("foo", "foo"));
    when(jedis.hdel(configs.getBackplane().getWorkersHashName() + "_storage", "foo"))
        .thenReturn(1L);
    RedisShardBackplane backplane = createBackplane("invalid-protobuf-worker-removed-test");
    backplane.start("startTime/test:0000");

    assertThat(backplane.getStorageWorkers()).isEmpty();
    verify(jedis, times(1)).hdel(configs.getBackplane().getWorkersHashName() + "_storage", "foo");
    ArgumentCaptor<String> changeCaptor = ArgumentCaptor.forClass(String.class);
    verify(jedis, times(1))
        .publish(eq(configs.getBackplane().getWorkerChannel()), changeCaptor.capture());
    String json = changeCaptor.getValue();
    WorkerChange.Builder builder = WorkerChange.newBuilder();
    JsonFormat.parser().merge(json, builder);
    WorkerChange workerChange = builder.build();
    assertThat(workerChange.getName()).isEqualTo("foo");
    assertThat(workerChange.getTypeCase()).isEqualTo(WorkerChange.TypeCase.REMOVE);
  }

  OperationChange verifyChangePublished(String channel, UnifiedJedis jedis) throws IOException {
    ArgumentCaptor<String> changeCaptor = ArgumentCaptor.forClass(String.class);
    verify(jedis, times(1)).publish(eq(channel), changeCaptor.capture());
    return parseOperationChange(changeCaptor.getValue());
  }

  String operationName(String name) {
    return "Operation:" + name;
  }

  @Test
  public void prequeueUpdatesOperationPrequeuesAndPublishes() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("prequeue-operation-test");
    backplane.start("startTime/test:0000");

    final String opName = "op";
    ExecuteEntry executeEntry = ExecuteEntry.newBuilder().setOperationName(opName).build();
    Operation op = Operation.newBuilder().setName(opName).build();
    backplane.prequeue(executeEntry, op);

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedis, times(1))
        .setex(
            operationName(opName),
            configs.getBackplane().getOperationExpire(),
            RedisShardBackplane.operationPrinter.print(op));
    verify(jedis, times(1))
        .lpush(
            configs.getBackplane().getPreQueuedOperationsListName(),
            JsonFormat.printer().print(executeEntry));
    OperationChange opChange = verifyChangePublished(backplane.operationChannel(opName), jedis);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
  }

  @Test
  public void queuingPublishes() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("requeue-operation-test");
    backplane.start("startTime/test:0000");

    final String opName = "op";
    backplane.queueing(opName);

    verify(mockJedisClusterFactory, times(1)).get();
    OperationChange opChange = verifyChangePublished(backplane.operationChannel(opName), jedis);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
  }

  @Test
  public void requeueDispatchedOperationQueuesAndPublishes() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("requeue-operation-test");
    backplane.start("startTime/test:0000");

    final String opName = "op";
    when(jedis.hdel(configs.getBackplane().getDispatchedOperationsHashName(), opName))
        .thenReturn(1L);

    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName("op").build())
            .build();
    backplane.requeueDispatchedOperation(queueEntry);

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedis, times(1)).hdel(configs.getBackplane().getDispatchedOperationsHashName(), opName);
    verify(jedis, times(1))
        .lpush(
            configs.getBackplane().getQueuedOperationsListName(),
            JsonFormat.printer().print(queueEntry));
    OperationChange opChange = verifyChangePublished(backplane.operationChannel(opName), jedis);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
  }

  @Test
  public void dispatchedOperationsShowProperRequeueAmount0to1()
      throws IOException, InterruptedException {
    // ARRANGE
    int STARTING_REQUEUE_AMOUNT = 0;
    int REQUEUE_AMOUNT_WHEN_DISPATCHED = 0;
    int REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE = 1;

    // create a backplane
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("requeue-operation-test");
    backplane.start("startTime/test:0000");

    // ARRANGE
    // Assume the operation queue is already populated with a first-time operation.
    // this means the operation's requeue amount will be 0.
    // The jedis cluser is also mocked to assume success on other operations.
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName("op").build())
            .setRequeueAttempts(STARTING_REQUEUE_AMOUNT)
            .build();
    String queueEntryJson = JsonFormat.printer().print(queueEntry);
    when(jedis.lmove(any(String.class), any(String.class), eq(RIGHT), eq(LEFT)))
        .thenReturn(queueEntryJson);

    // PRE-ASSERT
    when(jedis.hsetnx(any(String.class), any(String.class), any(String.class)))
        .thenAnswer(
            args -> {
              // Extract the operation that was dispatched
              String dispatchedOperationJson = args.getArgument(2);
              DispatchedOperation.Builder dispatchedOperationBuilder =
                  DispatchedOperation.newBuilder();
              JsonFormat.parser().merge(dispatchedOperationJson, dispatchedOperationBuilder);
              DispatchedOperation dispatchedOperation = dispatchedOperationBuilder.build();

              assertThat(dispatchedOperation.getQueueEntry().getRequeueAttempts())
                  .isEqualTo(REQUEUE_AMOUNT_WHEN_DISPATCHED);

              return 1L;
            });

    // ACT
    // dispatch the operation and test properties of the QueueEntry and internal jedis calls.
    List<Platform.Property> properties = new ArrayList<>();
    QueueEntry readyForRequeue = backplane.dispatchOperation(properties);

    // ASSERT
    assertThat(readyForRequeue.getRequeueAttempts())
        .isEqualTo(REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE);
  }

  @Test
  public void dispatchedOperationsShowProperRequeueAmount1to2()
      throws IOException, InterruptedException {
    // ARRANGE
    int STARTING_REQUEUE_AMOUNT = 1;
    int REQUEUE_AMOUNT_WHEN_DISPATCHED = 1;
    int REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE = 2;

    // create a backplane
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("requeue-operation-test");
    backplane.start("startTime/test:0000");

    // Assume the operation queue is already populated from a first re-queue.
    // this means the operation's requeue amount will be 1.
    // The jedis cluser is also mocked to assume success on other operations.
    QueueEntry queueEntry =
        QueueEntry.newBuilder()
            .setExecuteEntry(ExecuteEntry.newBuilder().setOperationName("op").build())
            .setRequeueAttempts(STARTING_REQUEUE_AMOUNT)
            .build();
    String queueEntryJson = JsonFormat.printer().print(queueEntry);
    when(jedis.lmove(any(String.class), any(String.class), eq(RIGHT), eq(LEFT)))
        .thenReturn(queueEntryJson);

    // PRE-ASSERT
    when(jedis.hsetnx(any(String.class), any(String.class), any(String.class)))
        .thenAnswer(
            args -> {
              // Extract the operation that was dispatched
              String dispatchedOperationJson = args.getArgument(2);
              DispatchedOperation.Builder dispatchedOperationBuilder =
                  DispatchedOperation.newBuilder();
              JsonFormat.parser().merge(dispatchedOperationJson, dispatchedOperationBuilder);
              DispatchedOperation dispatchedOperation = dispatchedOperationBuilder.build();

              assertThat(dispatchedOperation.getQueueEntry().getRequeueAttempts())
                  .isEqualTo(REQUEUE_AMOUNT_WHEN_DISPATCHED);

              return 1L;
            });

    // ACT
    // dispatch the operation and test properties of the QueueEntry and internal jedis calls.
    List<Platform.Property> properties = new ArrayList<>();
    QueueEntry readyForRequeue = backplane.dispatchOperation(properties);

    // ASSERT
    assertThat(readyForRequeue.getRequeueAttempts())
        .isEqualTo(REQUEUE_AMOUNT_WHEN_READY_TO_REQUEUE);
  }

  @Test
  public void completeOperationUndispatches() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("complete-operation-test");
    backplane.start("startTime/test:0000");

    final String opName = "op";

    backplane.completeOperation(opName);

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedis, times(1)).hdel(configs.getBackplane().getDispatchedOperationsHashName(), opName);
  }

  @Test
  @Ignore
  public void deleteOperationDeletesAndPublishes() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("delete-operation-test");
    backplane.start("startTime/test:0000");

    final String opName = "op";

    backplane.deleteOperation(opName);

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedis, times(1)).hdel(configs.getBackplane().getDispatchedOperationsHashName(), opName);
    verify(jedis, times(1)).del(operationName(opName));
    OperationChange opChange = verifyChangePublished(backplane.operationChannel(opName), jedis);
    assertThat(opChange.hasReset()).isTrue();
    assertThat(opChange.getReset().getOperation().getName()).isEqualTo(opName);
  }

  @Test
  public void invocationsCanBeBlacklisted() throws IOException {
    UUID toolInvocationId = UUID.randomUUID();
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    String invocationBlacklistKey =
        configs.getBackplane().getInvocationBlacklistPrefix() + ":" + toolInvocationId;
    when(jedis.exists(invocationBlacklistKey)).thenReturn(true);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("invocation-blacklist-test");
    backplane.start("startTime/test:0000");

    assertThat(
            backplane.isBlacklisted(
                RequestMetadata.newBuilder()
                    .setToolInvocationId(toolInvocationId.toString())
                    .build()))
        .isTrue();

    verify(mockJedisClusterFactory, times(1)).get();
    verify(jedis, times(1)).exists(invocationBlacklistKey);
  }

  @Test
  public void testGetWorkersStartTime() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("workers-starttime-test");
    backplane.start("startTime/test:0000");

    Set<String> workerNames = ImmutableSet.of("worker1", "worker2", "missing_worker");

    String storageWorkerKey = configs.getBackplane().getWorkersHashName() + "_storage";
    Map<String, String> workersJson =
        Map.of(
            "worker1",
                "{\"endpoint\": \"worker1\", \"expireAt\": \"9999999999999\", \"workerType\": 3, \"firstRegisteredAt\": \"1685292624000\"}",
            "worker2",
                "{\"endpoint\": \"worker2\", \"expireAt\": \"9999999999999\", \"workerType\": 3, \"firstRegisteredAt\": \"1685282624000\"}");
    when(jedis.hgetAll(storageWorkerKey)).thenReturn(workersJson);
    Map<String, Long> workersStartTime = backplane.getWorkersStartTimeInEpochSecs(workerNames);
    assertThat(workersStartTime.size()).isEqualTo(2);
    assertThat(workersStartTime.get("worker1")).isEqualTo(1685292624L);
    assertThat(workersStartTime.get("worker2")).isEqualTo(1685282624L);
    assertThat(workersStartTime.get("missing_worker")).isNull();
  }

  @Test
  public void getDigestInsertTime() throws IOException {
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    RedisShardBackplane backplane = createBackplane("digest-inserttime-test");
    backplane.start("startTime/test:0000");
    long ttl = 3600L;
    long expirationInSecs = configs.getBackplane().getCasExpire();
    when(jedis.ttl("ContentAddressableStorage:abc/0")).thenReturn(ttl);

    Digest digest = Digest.newBuilder().setHash("abc").build();

    Long insertTimeInSecs = backplane.getDigestInsertTime(digest);

    // Assuming there could be at most 2s delay in execution of both
    // `Instant.now().getEpochSecond()` call.
    assertThat(insertTimeInSecs)
        .isGreaterThan(Instant.now().getEpochSecond() - expirationInSecs + ttl - 2);
    assertThat(insertTimeInSecs).isAtMost(Instant.now().getEpochSecond() - expirationInSecs + ttl);
  }

  @Test
  public void testAddWorker() throws IOException {
    ShardWorker shardWorker =
        ShardWorker.newBuilder().setWorkerType(3).setFirstRegisteredAt(1703065913000L).build();
    UnifiedJedis jedis = mock(UnifiedJedis.class);
    when(mockJedisClusterFactory.get()).thenReturn(jedis);
    when(jedis.hset(anyString(), anyString(), anyString())).thenReturn(1L);
    RedisShardBackplane backplane = createBackplane("add-worker-test");
    backplane.start("addWorker/test:0000");
    backplane.addWorker(shardWorker);
    verify(jedis, times(1))
        .hset(
            configs.getBackplane().getWorkersHashName() + "_storage",
            "",
            JsonFormat.printer().print(shardWorker));
    verify(jedis, times(1))
        .hset(
            configs.getBackplane().getWorkersHashName() + "_execute",
            "",
            JsonFormat.printer().print(shardWorker));
    verify(jedis, times(1)).publish(anyString(), anyString());
  }
}
