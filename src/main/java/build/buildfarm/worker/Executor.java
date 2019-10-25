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

package build.buildfarm.worker;

import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Iterables.transform;
import static build.buildfarm.v1test.ExecutionPolicy.PolicyCase.WRAPPER;
import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.DAYS;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.logging.Level.SEVERE;

import build.bazel.remote.execution.v2.ActionResult;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.ExecuteOperationMetadata;
import build.bazel.remote.execution.v2.ExecuteResponse;
import build.bazel.remote.execution.v2.ExecutionStage;
import build.bazel.remote.execution.v2.Platform;
import build.bazel.remote.execution.v2.Platform.Property;
import build.buildfarm.common.Write;
import build.buildfarm.common.Write.NullWrite;
import build.buildfarm.v1test.ExecutingOperationMetadata;
import build.buildfarm.v1test.ExecutionPolicy;
import com.google.common.base.Stopwatch;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.SettableFuture;
import com.google.longrunning.Operation;
import com.google.protobuf.Any;
import com.google.protobuf.ByteString;
import com.google.protobuf.Duration;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.Timestamps;
import com.google.rpc.Code;
import io.grpc.Deadline;
import java.nio.file.Path;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

class Executor implements Runnable {
  private static final int INCOMPLETE_EXIT_CODE = -1;
  private static final Logger logger = Logger.getLogger(Executor.class.getName());

  private final WorkerContext workerContext;
  private final OperationContext operationContext;
  private final ExecuteActionStage owner;
  private int exitCode = INCOMPLETE_EXIT_CODE;

  Executor(WorkerContext workerContext, OperationContext operationContext, ExecuteActionStage owner) {
    this.workerContext = workerContext;
    this.operationContext = operationContext;
    this.owner = owner;
  }

  private long runInterruptible(Stopwatch stopwatch) throws InterruptedException {
    ExecuteOperationMetadata metadata;
    try {
      metadata = operationContext.operation
          .getMetadata().unpack(ExecuteOperationMetadata.class);
    } catch (InvalidProtocolBufferException e) {
      logger.log(SEVERE, "invalid execute operation metadata", e);
      return 0;
    }
    ExecuteOperationMetadata executingMetadata = metadata.toBuilder()
        .setStage(ExecutionStage.Value.EXECUTING)
        .build();

    long startedAt = System.currentTimeMillis();

    Operation operation = operationContext.operation.toBuilder()
        .setMetadata(Any.pack(ExecutingOperationMetadata.newBuilder()
            .setStartedAt(startedAt)
            .setExecutingOn(workerContext.getName())
            .setExecuteOperationMetadata(executingMetadata)
            .setRequestMetadata(operationContext.queueEntry.getExecuteEntry().getRequestMetadata())
            .build()))
        .build();

    boolean operationUpdateSuccess = false;
    try {
      operationUpdateSuccess = workerContext.putOperation(operation, operationContext.action);
    } catch (IOException e) {
      logger.log(SEVERE, format("error putting operation %s as EXECUTING", operation.getName()), e);
    }

    if (!operationUpdateSuccess) {
      logger.warning(
          String.format(
              "Executor::run(%s): could not transition to EXECUTING",
              operation.getName()));
      owner.error().put(operationContext);
      return 0;
    }

    Duration timeout;
    if (operationContext.action.hasTimeout()) {
      timeout = operationContext.action.getTimeout();
    } else {
      timeout = null;
    }

    if (timeout == null && workerContext.hasDefaultActionTimeout()) {
      timeout = workerContext.getDefaultActionTimeout();
    }

    Deadline pollDeadline;
    if (timeout == null) {
      pollDeadline = Deadline.after(10, DAYS);
    } else {
      pollDeadline = Deadline.after(
          // 10s of padding for the timeout in question, so that we can guarantee cleanup
          (timeout.getSeconds() + 10) * 1000000 + timeout.getNanos() / 1000,
          MICROSECONDS);
    }

    workerContext.resumePoller(
        operationContext.poller,
        "Executor",
        operationContext.queueEntry,
        ExecutionStage.Value.EXECUTING,
        Thread.currentThread()::interrupt,
        pollDeadline);

    try {
      return executePolled(operation, timeout, stopwatch);
    } finally {
      operationContext.poller.pause();
    }
  }

  private void errorUninterruptibly(OperationContext operationContext) {
    try {
      owner.error().put(operationContext);
    } catch (InterruptedException e) {
      logger.log(SEVERE, "interrupted while erroring " + operationContext.operation.getName(), e);
      Thread.currentThread().interrupt();
    }
  }

  private long executePolled(
      Operation operation,
      Duration timeout,
      Stopwatch stopwatch) throws InterruptedException {
    /* execute command */
    workerContext.logInfo("Executor: Operation " + operation.getName() + " Executing command");

    Platform platform = operationContext.command.getPlatform();
    ImmutableList.Builder<ExecutionPolicy> policies = ImmutableList.builder();
    ExecutionPolicy defaultPolicy = workerContext.getExecutionPolicy("");
    if (defaultPolicy != null) {
      policies.add(defaultPolicy);
    }
    for (Property property : platform.getPropertiesList()) {
      if (property.getName().equals("execution-policy")) {
        policies.add(workerContext.getExecutionPolicy(property.getValue()));
      }
    }

    ActionResult.Builder resultBuilder = operationContext.executeResponse
        .getResultBuilder();
    resultBuilder.getExecutionMetadataBuilder()
        .setExecutionStartTimestamp(Timestamps.fromMillis(System.currentTimeMillis()));

    Code statusCode;
    try {
      statusCode = executeCommand(
          operation.getName(),
          operationContext.execDir,
          operationContext.command,
          timeout,
          "", // executingMetadata.getStdoutStreamName(),
          "", // executingMetadata.getStderrStreamName(),
          resultBuilder,
          policies.build());
    } catch (IOException e) {
      logger.log(SEVERE, "error executing operation " + operation.getName(), e);
      operationContext.poller.pause();
      errorUninterruptibly(operationContext);
      return 0;
    }

    resultBuilder.getExecutionMetadataBuilder()
        .setExecutionCompletedTimestamp(Timestamps.fromMillis(System.currentTimeMillis()));
    long executeUSecs = stopwatch.elapsed(MICROSECONDS);

    logger.info(
        String.format(
            "Executor::executeCommand(%s): Completed command: exit code %d",
            operation.getName(),
            resultBuilder.getExitCode()));

    operationContext.executeResponse.getStatusBuilder()
        .setCode(statusCode.getNumber());
    OperationContext reportOperationContext = operationContext.toBuilder()
				.setOperation(operation)
				.build();
    boolean claimed = owner.output().claim();
    operationContext.poller.pause();
    if (claimed) {
      try {
        owner.output().put(reportOperationContext);
      } catch (InterruptedException e) {
        owner.output().release();
        throw e;
      }
    } else {
      // FIXME we need to release the action root
      workerContext.logInfo("Executor: Operation " + operation.getName() + " Failed to claim output");

      errorUninterruptibly(operationContext);
    }
    return stopwatch.elapsed(MICROSECONDS) - executeUSecs;
  }

  @Override
  public void run() {
    long stallUSecs = 0;
    Stopwatch stopwatch = Stopwatch.createStarted();
    String operationName = operationContext.operation.getName();
    boolean wasInterrupted = false;
    try {
      stallUSecs = runInterruptible(stopwatch);
    } catch (InterruptedException e) {
      /* we can be interrupted when the poller fails */
      wasInterrupted = Thread.interrupted();
      errorUninterruptibly(operationContext);
    } catch (Exception e) {
      logger.log(SEVERE, "errored during execution of " + operationName, e);
      errorUninterruptibly(operationContext);
      throw e;
    } finally {
      wasInterrupted = Thread.interrupted() || wasInterrupted;
      try {
        owner.releaseExecutor(
            operationName,
            stopwatch.elapsed(MICROSECONDS),
            stallUSecs,
            exitCode);
      } finally {
        if (wasInterrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }
  }

  private Code executeCommand(
      String operationName,
      Path execDir,
      Command command,
      Duration timeout,
      String stdoutStreamName,
      String stderrStreamName,
      ActionResult.Builder resultBuilder,
      Iterable<ExecutionPolicy> policies)
      throws IOException, InterruptedException {
    ImmutableList.Builder<String> arguments = ImmutableList.builder();
    arguments.addAll(
        transform(
            filter(policies, (policy) -> policy.getPolicyCase() == WRAPPER),
            (policy) -> policy.getWrapper().getPath()));
    arguments.addAll(command.getArgumentsList());

    ProcessBuilder processBuilder =
        new ProcessBuilder(arguments.build())
            .directory(execDir.toAbsolutePath().toFile());

    Map<String, String> environment = processBuilder.environment();
    environment.clear();
    for (Command.EnvironmentVariable environmentVariable : command.getEnvironmentVariablesList()) {
      environment.put(environmentVariable.getName(), environmentVariable.getValue());
    }

    final Write stdoutWrite, stderrWrite;

    if (stdoutStreamName != null && !stdoutStreamName.isEmpty() && workerContext.getStreamStdout()) {
      stdoutWrite = workerContext.getOperationStreamWrite(stdoutStreamName);
    } else {
      stdoutWrite = new NullWrite();
    }
    if (stderrStreamName != null && !stderrStreamName.isEmpty() && workerContext.getStreamStderr()) {
      stderrWrite = workerContext.getOperationStreamWrite(stderrStreamName);
    } else {
      stderrWrite = new NullWrite();
    }

    long startNanoTime = System.nanoTime();
    Process process;
    try {
      synchronized (this) {
        process = processBuilder.start();
      }
      process.getOutputStream().close();
    } catch(IOException e) {
      logger.log(SEVERE, "error starting process for " + operationName, e);
      // again, should we do something else here??
      resultBuilder.setExitCode(INCOMPLETE_EXIT_CODE);
      return Code.INVALID_ARGUMENT;
    }

    stdoutWrite.reset();
    stderrWrite.reset();
    ByteStringWriteReader stdoutReader = new ByteStringWriteReader(
        process.getInputStream(), stdoutWrite);
    ByteStringWriteReader stderrReader = new ByteStringWriteReader(
        process.getErrorStream(), stderrWrite);

    Thread stdoutReaderThread = new Thread(stdoutReader);
    Thread stderrReaderThread = new Thread(stderrReader);
    stdoutReaderThread.start();
    stderrReaderThread.start();

    Code statusCode = Code.OK;
    try {
      if (timeout == null) {
        exitCode = process.waitFor();
      } else {
        long timeoutNanos = timeout.getSeconds() * 1000000000L + timeout.getNanos();
        long remainingNanoTime = timeoutNanos - (System.nanoTime() - startNanoTime);
        if (process.waitFor(remainingNanoTime, TimeUnit.NANOSECONDS)) {
          exitCode = process.exitValue();
        } else {
          logger.info("process timed out for " + operationName);
          process.destroy();
          if (!process.waitFor(1, TimeUnit.SECONDS)) {
            logger.info(format("process did not respond to termination for %s, killing it", operationName));
            process.destroyForcibly();
            process.waitFor(100, TimeUnit.MILLISECONDS); // fair trade, i think
          }
          statusCode = Code.DEADLINE_EXCEEDED;
        }
      }
    } catch (InterruptedException e) {
      process.destroy();
      if (!process.waitFor(1, TimeUnit.SECONDS)) {
        process.destroyForcibly();
        process.waitFor(100, TimeUnit.MILLISECONDS);
      }
      throw e;
    }
    stdoutReaderThread.join();
    stderrReaderThread.join();
    resultBuilder
        .setExitCode(exitCode)
        .setStdoutRaw(stdoutReader.getData())
        .setStderrRaw(stderrReader.getData());
    return statusCode;
  }
}
