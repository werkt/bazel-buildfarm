// Copyright 2017 The Bazel Authors. All rights reserved.
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

package build.buildfarm.worker.shard;

import static build.buildfarm.cas.ContentAddressableStorages.createGrpcCAS;
import static build.buildfarm.common.config.Backplane.BACKPLANE_TYPE.SHARD;
import static build.buildfarm.common.io.Utils.formatIOError;
import static build.buildfarm.common.io.Utils.getUser;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;
import static java.util.concurrent.TimeUnit.SECONDS;
import static java.util.logging.Level.INFO;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.buildfarm.admin.aws.AwsAdmin;
import build.buildfarm.backplane.Backplane;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.cas.ContentAddressableStorage.Blob;
import build.buildfarm.cas.MemoryCAS;
import build.buildfarm.cas.cfc.CASFileCache;
import build.buildfarm.common.DigestUtil;
import build.buildfarm.common.InputStreamFactory;
import build.buildfarm.common.config.BuildfarmConfigs;
import build.buildfarm.common.services.ByteStreamService;
import build.buildfarm.common.services.ContentAddressableStorageService;
import build.buildfarm.instance.Instance;
import build.buildfarm.instance.shard.RedisShardBackplane;
import build.buildfarm.instance.shard.RemoteInputStreamFactory;
import build.buildfarm.instance.shard.WorkerStubs;
import build.buildfarm.metrics.prometheus.PrometheusPublisher;
import build.buildfarm.v1test.ShardWorker;
import build.buildfarm.worker.ExecuteActionStage;
import build.buildfarm.worker.FuseCAS;
import build.buildfarm.worker.InputFetchStage;
import build.buildfarm.worker.MatchStage;
import build.buildfarm.worker.Pipeline;
import build.buildfarm.worker.PipelineStage;
import build.buildfarm.worker.PutOperationStage;
import build.buildfarm.worker.ReportResultStage;
import com.google.common.cache.LoadingCache;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.google.devtools.common.options.OptionsParsingException;
import com.google.longrunning.Operation;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.health.v1.HealthCheckResponse.ServingStatus;
import io.grpc.services.HealthStatusManager;
import io.prometheus.client.Counter;
import io.prometheus.client.Gauge;
import java.io.File;
import java.io.IOException;
import java.nio.file.FileSystem;
import java.nio.file.Path;
import java.nio.file.attribute.UserPrincipal;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.logging.Level;
import javax.annotation.Nullable;
import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import javax.naming.ConfigurationException;
import lombok.extern.java.Log;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.ComponentScan;

@Log
@SpringBootApplication
@ComponentScan("build.buildfarm")
public class Worker {
  private static final java.util.logging.Logger nettyLogger =
      java.util.logging.Logger.getLogger("io.grpc.netty");
  private static final Counter healthCheckMetric =
      Counter.build()
          .name("health_check")
          .labelNames("lifecycle")
          .help("Service health check.")
          .register();
  private static final Counter workerPausedMetric =
      Counter.build().name("worker_paused").help("Worker paused.").register();
  private static final Gauge executionSlotsTotal =
      Gauge.build()
          .name("execution_slots_total")
          .help("Total execution slots configured on worker.")
          .register();
  private static final Gauge inputFetchSlotsTotal =
      Gauge.build()
          .name("input_fetch_slots_total")
          .help("Total input fetch slots configured on worker.")
          .register();

  private static final int shutdownWaitTimeInSeconds = 10;

  private static BuildfarmConfigs configs = BuildfarmConfigs.getInstance();

  private boolean inGracefulShutdown = false;
  private boolean isPaused = false;

  private ShardWorkerInstance instance;

  @SuppressWarnings("deprecation")
  private final HealthStatusManager healthStatusManager = new HealthStatusManager();

  private Server server;
  private Path root;
  private DigestUtil digestUtil;
  private ExecFileSystem execFileSystem;
  private Pipeline pipeline;
  private Backplane backplane;
  private LoadingCache<String, Instance> workerStubs;
  @Autowired private AwsAdmin awsAdmin;

  /**
   * The method will prepare the worker for graceful shutdown and send out grpc request to disable
   * scale in protection when the worker is ready. If unexpected errors happened, it will cancel the
   * graceful shutdown progress make the worker available again.
   */
  public void prepareWorkerForGracefulShutdown() {
    inGracefulShutdown = true;
    log.log(
        Level.INFO,
        "The current worker will not be registered again and should be shutdown gracefully!");
    pipeline.stopMatchingOperations();
    int scanRate = 30; // check every 30 seconds
    int timeWaited = 0;
    int timeOut = 60 * 15; // 15 minutes

    try {
      while (!pipeline.isEmpty() && timeWaited < timeOut) {
        SECONDS.sleep(scanRate);
        timeWaited += scanRate;
        log.log(INFO, String.format("Pipeline is still not empty after %d seconds.", timeWaited));
      }
    } catch (InterruptedException e) {
      log.log(Level.SEVERE, "The worker gracefully shutdown is interrupted: " + e.getMessage());
    } finally {
      // make a grpc call to disable scale protection
      String clusterEndpoint = configs.getServer().getAdmin().getClusterEndpoint();
      log.log(
          INFO,
          String.format(
              "It took the worker %d seconds to %s",
              timeWaited,
              pipeline.isEmpty() ? "finish all actions" : "but still cannot finish all actions"));
      try {
        awsAdmin.disableHostScaleInProtection(clusterEndpoint, configs.getWorker().getPublicName());
      } catch (Exception e) {
        log.log(
            SEVERE,
            String.format(
                "gRPC call to AdminService to disable scale in protection failed with exception: %s and stacktrace %s",
                e.getMessage(), Arrays.toString(e.getStackTrace())));
        // Gracefully shutdown cannot be performed successfully because of error in
        // AdminService side. Under this scenario, the worker has to be added back to the worker
        // pool.
        inGracefulShutdown = false;
      }
    }
  }

  private Operation stripOperation(Operation operation) {
    return instance.stripOperation(operation);
  }

  private Operation stripQueuedOperation(Operation operation) {
    return instance.stripQueuedOperation(operation);
  }

  private Server createServer(
      ServerBuilder<?> serverBuilder,
      ContentAddressableStorage storage,
      Instance instance,
      Pipeline pipeline,
      ShardWorkerContext context) {
    serverBuilder.addService(healthStatusManager.getHealthService());
    serverBuilder.addService(new ContentAddressableStorageService(instance));
    serverBuilder.addService(new ByteStreamService(instance));
    serverBuilder.addService(new ShutDownWorkerGracefully(this));

    // We will build a worker's server based on it's capabilities.
    // A worker that is capable of execution will construct an execution pipeline.
    // It will use various execution phases for it's profile service.
    // On the other hand, a worker that is only capable of CAS storage does not need a pipeline.
    if (configs.getWorker().getCapabilities().isExecution()) {
      PipelineStage completeStage =
          new PutOperationStage((operation) -> context.deactivate(operation.getName()));
      PipelineStage errorStage = completeStage; /* new ErrorStage(); */
      PipelineStage reportResultStage = new ReportResultStage(context, completeStage, errorStage);
      PipelineStage executeActionStage =
          new ExecuteActionStage(context, reportResultStage, errorStage);
      PipelineStage inputFetchStage =
          new InputFetchStage(context, executeActionStage, new PutOperationStage(context::requeue));
      PipelineStage matchStage = new MatchStage(context, inputFetchStage, errorStage);

      pipeline.add(matchStage, 4);
      pipeline.add(inputFetchStage, 3);
      pipeline.add(executeActionStage, 2);
      pipeline.add(reportResultStage, 1);

      serverBuilder.addService(
          new WorkerProfileService(
              storage, inputFetchStage, executeActionStage, context, completeStage, backplane));
    }

    return serverBuilder.build();
  }

  @SuppressWarnings("SynchronizationOnLocalVariableOrMethodParameter")
  private ExecFileSystem createFuseExecFileSystem(
      InputStreamFactory remoteInputStreamFactory, ContentAddressableStorage storage) {
    InputStreamFactory storageInputStreamFactory =
        (compressor, digest, offset) -> {
          checkArgument(compressor == Compressor.Value.IDENTITY);
          return storage.get(digest).getData().substring((int) offset).newInput();
        };

    InputStreamFactory localPopulatingInputStreamFactory =
        (compressor, blobDigest, offset) -> {
          // FIXME use write
          ByteString content =
              ByteString.readFrom(remoteInputStreamFactory.newInput(compressor, blobDigest, offset));

          // needs some treatment for compressor
          if (offset == 0) {
            // extra computations
            Blob blob = new Blob(content, digestUtil);
            // here's hoping that our digest matches...
            try {
              storage.put(blob);
            } catch (InterruptedException e) {
              throw new IOException(e);
            }
          }

          return content.newInput();
        };
    return new FuseExecFileSystem(
        root,
        new FuseCAS(
            root,
            new EmptyInputStreamFactory(
                new FailoverInputStreamFactory(
                    storageInputStreamFactory, localPopulatingInputStreamFactory))),
        storage);
  }

  private @Nullable UserPrincipal getOwner(FileSystem fileSystem) throws ConfigurationException {
    try {
      return getUser(configs.getWorker().getExecOwner(), fileSystem);
    } catch (IOException e) {
      ConfigurationException configException =
          new ConfigurationException("Could not locate exec_owner");
      configException.initCause(e);
      throw configException;
    }
  }

  private ExecFileSystem createExecFileSystem(
      InputStreamFactory remoteInputStreamFactory,
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder,
      ContentAddressableStorage storage)
      throws ConfigurationException {
    checkState(storage != null, "no exec fs cas specified");
    if (storage instanceof CASFileCache) {
      CASFileCache cfc = (CASFileCache) storage;
      UserPrincipal owner = getOwner(cfc.getRoot().getFileSystem());
      return createCFCExecFileSystem(removeDirectoryService, accessRecorder, cfc, owner);
    } else {
      // FIXME not the only fuse backing capacity...
      return createFuseExecFileSystem(remoteInputStreamFactory, storage);
    }
  }

  private ContentAddressableStorage createStorage(
      InputStreamFactory remoteInputStreamFactory,
      ExecutorService removeDirectoryService,
      Executor accessRecorder,
      ContentAddressableStorage delegate)
      throws ConfigurationException {
    switch (configs.getWorker().getCas().getType()) {
      default:
        throw new IllegalArgumentException("Invalid cas type specified");
      case MEMORY:
      case FUSE: // FIXME have FUSE refer to a name for storage backing, and topo
        return new MemoryCAS(
            configs.getWorker().getCas().getMaxSizeBytes(), this::onStoragePut, delegate);
      case GRPC:
        checkState(delegate == null, "grpc cas cannot delegate");
        return createGrpcCAS();
      case FILESYSTEM:
        return new ShardCASFileCache(
            remoteInputStreamFactory,
            root.resolve(configs.getWorker().getCas().getValidPath(root)),
            configs.getWorker().getCas().getMaxSizeBytes(),
            configs.getWorker().getCas().getMaxEntrySizeBytes(),
            configs.getWorker().getHexBucketLevels(),
            configs.getWorker().getCas().isFileDirectoriesIndexInMemory(),
            digestUtil,
            removeDirectoryService,
            accessRecorder,
            this::onStoragePut,
            delegate == null ? this::onStorageExpire : (digests) -> {},
            delegate);
    }
  }

  private ExecFileSystem createCFCExecFileSystem(
      ExecutorService removeDirectoryService,
      ExecutorService accessRecorder,
      CASFileCache fileCache,
      @Nullable UserPrincipal owner) {
    return new CFCExecFileSystem(
        root,
        fileCache,
        owner,
        configs.getWorker().isLinkInputDirectories(),
        configs.getWorker().getRealInputDirectories(),
        removeDirectoryService,
        accessRecorder
        /* deadlineAfter=*/
        /* deadlineAfterUnits=*/ );
  }

  private void onStoragePut(Digest digest) {
    try {
      // if the worker is a CAS member, it can send/modify blobs in the backplane.
      if (configs.getWorker().getCapabilities().isCas()) {
        backplane.addBlobLocation(digest, configs.getWorker().getPublicName());
      }
    } catch (IOException e) {
      throw Status.fromThrowable(e).asRuntimeException();
    }
  }

  private void onStorageExpire(Iterable<Digest> digests) {
    if (configs.getWorker().getCapabilities().isCas()) {
      try {
        // if the worker is a CAS member, it can send/modify blobs in the backplane.
        backplane.removeBlobsLocation(digests, configs.getWorker().getPublicName());
      } catch (IOException e) {
        throw Status.fromThrowable(e).asRuntimeException();
      }
    }
  }

  private void removeWorker(String name) {
    try {
      backplane.removeWorker(name, "removing self prior to initialization");
    } catch (IOException e) {
      Status status = Status.fromThrowable(e);
      if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
        throw status.asRuntimeException();
      }
      log.log(INFO, "backplane was unavailable or overloaded, deferring removeWorker");
    }
  }

  private void addBlobsLocation(List<Digest> digests, String name) {
    while (!backplane.isStopped()) {
      try {
        backplane.addBlobsLocation(digests, name);
        return;
      } catch (IOException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
          throw status.asRuntimeException();
        }
      }
    }
    throw Status.UNAVAILABLE.withDescription("backplane was stopped").asRuntimeException();
  }

  private void addWorker(ShardWorker worker) {
    while (!backplane.isStopped()) {
      try {
        backplane.addWorker(worker);
        return;
      } catch (IOException e) {
        Status status = Status.fromThrowable(e);
        if (status.getCode() != Code.UNAVAILABLE && status.getCode() != Code.DEADLINE_EXCEEDED) {
          throw status.asRuntimeException();
        }
      }
    }
    throw Status.UNAVAILABLE.withDescription("backplane was stopped").asRuntimeException();
  }

  private void startFailsafeRegistration() {
    String endpoint = configs.getWorker().getPublicName();
    ShardWorker.Builder worker = ShardWorker.newBuilder().setEndpoint(endpoint);
    worker.setWorkerType(configs.getWorker().getWorkerType());
    int registrationIntervalMillis = 10000;
    int registrationOffsetMillis = registrationIntervalMillis * 3;
    new Thread(
            new Runnable() {
              long workerRegistrationExpiresAt = 0;

              ShardWorker nextRegistration(long now) {
                return worker.setExpireAt(now + registrationOffsetMillis).build();
              }

              long nextInterval(long now) {
                return now + registrationIntervalMillis;
              }

              boolean isWorkerPausedFromNewWork() {
                try {
                  File pausedFile = new File(configs.getWorker().getRoot() + "/.paused");
                  if (pausedFile.exists() && !isPaused) {
                    isPaused = true;
                    log.log(Level.INFO, "The current worker is paused from taking on new work!");
                    pipeline.stopMatchingOperations();
                    workerPausedMetric.inc();
                  }
                } catch (Exception e) {
                  log.log(Level.WARNING, "Could not open .paused file.", e);
                }
                return isPaused;
              }

              void registerIfExpired() {
                long now = System.currentTimeMillis();
                if (now >= workerRegistrationExpiresAt
                    && !inGracefulShutdown
                    && !isWorkerPausedFromNewWork()) {
                  // worker must be registered to match
                  addWorker(nextRegistration(now));
                  // update every 10 seconds
                  workerRegistrationExpiresAt = nextInterval(now);
                }
              }

              @Override
              public void run() {
                try {
                  while (!server.isShutdown()) {
                    registerIfExpired();
                    SECONDS.sleep(1);
                  }
                } catch (InterruptedException e) {
                  // ignore
                } finally {
                  try {
                    stop();
                  } catch (InterruptedException ie) {
                    log.log(SEVERE, "interrupted while stopping worker", ie);
                    // ignore
                  }
                }
              }
            })
        .start();
  }

  public void start() throws ConfigurationException, InterruptedException, IOException {
    String session = UUID.randomUUID().toString();
    ServerBuilder<?> serverBuilder = ServerBuilder.forPort(configs.getWorker().getPort());
    String identifier = "buildfarm-worker-" + configs.getWorker().getPublicName() + "-" + session;
    root = configs.getWorker().getValidRoot();
    if (configs.getWorker().getPublicName().isEmpty()) {
      throw new ConfigurationException("worker's public name should not be empty");
    }

    digestUtil = new DigestUtil(configs.getDigestFunction());

    if (SHARD.equals(configs.getBackplane().getType())) {
      backplane =
          new RedisShardBackplane(identifier, this::stripOperation, this::stripQueuedOperation);
      backplane.start(configs.getWorker().getPublicName());
    } else {
      throw new IllegalArgumentException("Shard Backplane not set in config");
    }

    workerStubs =
        WorkerStubs.create(
            digestUtil,
            Duration.newBuilder().setSeconds(configs.getServer().getGrpcTimeout()).build());

    ExecutorService removeDirectoryService =
        newFixedThreadPool(
            /* nThreads=*/ 32,
            new ThreadFactoryBuilder().setNameFormat("remove-directory-pool-%d").build());
    ExecutorService accessRecorder = newSingleThreadExecutor();

    InputStreamFactory remoteInputStreamFactory =
        new RemoteInputStreamFactory(
            configs.getWorker().getPublicName(),
            backplane,
            new Random(),
            workerStubs,
            (worker, t, context) -> {});
    ContentAddressableStorage storage =
        createStorage(remoteInputStreamFactory, removeDirectoryService, accessRecorder, null);
    execFileSystem =
        createExecFileSystem(
            remoteInputStreamFactory, removeDirectoryService, accessRecorder, storage);

    instance =
        new ShardWorkerInstance(
            configs.getWorker().getPublicName(), digestUtil, backplane, storage);

    // Create the appropriate writer for the context
    CasWriter writer;
    if (!configs.getWorker().getCapabilities().isCas()) {
      writer = new RemoteCasWriter(backplane.getWorkers(), workerStubs);
    } else {
      writer = new LocalCasWriter(execFileSystem);
    }

    ShardWorkerContext context =
        new ShardWorkerContext(
            configs.getWorker().getPublicName(),
            Duration.newBuilder().setSeconds(configs.getWorker().getOperationPollPeriod()).build(),
            backplane::pollOperation,
            configs.getWorker().getInputFetchStageWidth(),
            configs.getWorker().getExecuteStageWidth(),
            configs.getWorker().getInputFetchDeadline(),
            backplane,
            execFileSystem,
            new EmptyInputStreamFactory(
                new FailoverInputStreamFactory(
                    execFileSystem.getStorage(), remoteInputStreamFactory)),
            Arrays.asList(configs.getWorker().getExecutionPolicies()),
            instance,
            Duration.newBuilder().setSeconds(configs.getDefaultActionTimeout()).build(),
            Duration.newBuilder().setSeconds(configs.getMaximumActionTimeout()).build(),
            configs.getWorker().getDefaultMaxCores(),
            configs.getWorker().isLimitGlobalExecution(),
            configs.getWorker().isOnlyMulticoreTests(),
            configs.getWorker().isAllowBringYourOwnContainer(),
            configs.getWorker().isErrorOperationRemainingResources(),
            writer);

    pipeline = new Pipeline();
    server = createServer(serverBuilder, storage, instance, pipeline, context);

    removeWorker(configs.getWorker().getPublicName());

    boolean skipLoad = configs.getWorker().getCas().isSkipLoad();
    execFileSystem.start(
        (digests) -> addBlobsLocation(digests, configs.getWorker().getPublicName()), skipLoad);

    server.start();
    healthStatusManager.setStatus(
        HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.SERVING);
    PrometheusPublisher.startHttpServer(configs.getPrometheusPort());
    // Not all workers need to be registered and visible in the backplane.
    // For example, a GPU worker may wish to perform work that we do not want to cache locally for
    // other workers.
    if (configs.getWorker().getCapabilities().isCas()) {
      startFailsafeRegistration();
    } else {
      log.log(INFO, "Skipping worker registration");
    }

    pipeline.start();
    healthCheckMetric.labels("start").inc();
    executionSlotsTotal.set(configs.getWorker().getExecuteStageWidth());
    inputFetchSlotsTotal.set(configs.getWorker().getInputFetchStageWidth());

    log.log(INFO, String.format("%s initialized", identifier));
  }

  @PreDestroy
  public void stop() throws InterruptedException {
    System.err.println("*** shutting down gRPC server since JVM is shutting down");
    PrometheusPublisher.stopHttpServer();
    boolean interrupted = Thread.interrupted();
    if (pipeline != null) {
      log.log(INFO, "Closing the pipeline");
      try {
        pipeline.close();
      } catch (InterruptedException e) {
        Thread.interrupted();
        interrupted = true;
      }
    }
    healthStatusManager.setStatus(
        HealthStatusManager.SERVICE_NAME_ALL_SERVICES, ServingStatus.NOT_SERVING);
    healthCheckMetric.labels("stop").inc();
    executionSlotsTotal.set(0);
    inputFetchSlotsTotal.set(0);
    if (execFileSystem != null) {
      log.log(INFO, "Stopping exec filesystem");
      execFileSystem.stop();
    }
    if (server != null) {
      log.log(INFO, "Shutting down the server");
      server.shutdown();

      try {
        server.awaitTermination(shutdownWaitTimeInSeconds, SECONDS);
      } catch (InterruptedException e) {
        interrupted = true;
        log.log(SEVERE, "interrupted while waiting for server shutdown", e);
      } finally {
        server.shutdownNow();
      }
    }
    if (backplane != null) {
      try {
        backplane.stop();
      } catch (InterruptedException e) {
        interrupted = true;
      }
    }
    if (workerStubs != null) {
      workerStubs.invalidateAll();
    }
    if (interrupted) {
      Thread.currentThread().interrupt();
      throw new InterruptedException();
    }
    System.err.println("*** server shut down");
  }

  @PostConstruct
  public void init() throws OptionsParsingException {
    // Only log severe log messages from Netty. Otherwise it logs warnings that look like this:
    //
    // 170714 08:16:28.552:WT 18 [io.grpc.netty.NettyServerHandler.onStreamError] Stream Error
    // io.netty.handler.codec.http2.Http2Exception$StreamException: Received DATA frame for an
    // unknown stream 11369
    nettyLogger.setLevel(SEVERE);

    try {
      start();
    } catch (IOException e) {
      System.err.println("error: " + formatIOError(e));
    } catch (InterruptedException e) {
      System.out.println("error: interrupted");
    } catch (ConfigurationException e) {
      throw new RuntimeException(e);
    }
  }

  public static void main(String[] args) throws ConfigurationException {
    configs = BuildfarmConfigs.loadWorkerConfigs(args);
    SpringApplication.run(Worker.class, args);
  }
}
