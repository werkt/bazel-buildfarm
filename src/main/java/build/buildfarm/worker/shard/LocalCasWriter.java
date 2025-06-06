// Copyright 2022 The Buildfarm Authors. All rights reserved.
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

import static java.util.concurrent.TimeUnit.DAYS;

import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.RequestMetadata;
import build.buildfarm.common.Write;
import build.buildfarm.common.function.IOSupplier;
import build.buildfarm.v1test.Digest;
import build.buildfarm.worker.ExecFileSystem;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import io.grpc.Status;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;

class LocalCasWriter implements CasWriter {
  private ExecFileSystem execFileSystem;

  public LocalCasWriter(ExecFileSystem execFileSystem) {
    this.execFileSystem = execFileSystem;
  }

  @Override
  public void write(Digest digest, Path file) throws IOException, InterruptedException {
    insertStream(digest, () -> Files.newInputStream(file));
  }

  @Override
  public void insertBlob(Digest digest, ByteString content)
      throws IOException, InterruptedException {
    insertStream(digest, content::newInput);
  }

  private Write getLocalWrite(Digest digest) throws IOException {
    return execFileSystem
        .getStorage()
        .getWrite(
            Compressor.Value.IDENTITY,
            digest,
            UUID.randomUUID(),
            RequestMetadata.getDefaultInstance());
  }

  private void insertStream(Digest digest, IOSupplier<InputStream> suppliedStream)
      throws IOException, InterruptedException {
    Write write = getLocalWrite(digest);

    try (OutputStream out =
            write.getOutput(/* deadlineAfter= */ 1, /* deadlineAfterUnits= */ DAYS, () -> {});
        InputStream in = suppliedStream.get()) {
      ByteStreams.copy(in, out);
    } catch (IOException e) {
      // should do something different here for IOException(InterruptedException)
      // yields:
      //
      // Jul 17, 2024 8:15:28 AM build.buildfarm.worker.ReportResultStage reportPolled
      // SEVERE: error uploading outputs for shard/executions/2cd29003-114d-46c8-b033-16e816d42e43
      // java.io.IOException: io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED
      //         at build.buildfarm.worker.shard.LocalCasWriter.insertStream(LocalCasWriter.java:76)
      //         at build.buildfarm.worker.shard.LocalCasWriter.write(LocalCasWriter.java:46)
      //         at build.buildfarm.worker.shard.ShardWorkerContext.insertFile(ShardWorkerContext.java:480)
      //         at build.buildfarm.worker.shard.ShardWorkerContext.uploadOutputFile(ShardWorkerContext.java:574)
      //         at build.buildfarm.worker.shard.ShardWorkerContext.uploadOutputs(ShardWorkerContext.java:765)
      //         at build.buildfarm.worker.ReportResultStage.reportPolled(ReportResultStage.java:120)
      //         at build.buildfarm.worker.ReportResultStage.tick(ReportResultStage.java:79)
      //         at build.buildfarm.worker.PipelineStage.iterate(PipelineStage.java:130)
      //         at build.buildfarm.worker.PipelineStage.runInterruptible(PipelineStage.java:48)
      //         at build.buildfarm.worker.PipelineStage.run(PipelineStage.java:61)
      //         at java.base/java.lang.Thread.run(Thread.java:1583)
      // Caused by: io.grpc.StatusRuntimeException: RESOURCE_EXHAUSTED
      //         at io.grpc.Status.asRuntimeException(Status.java:525)
      //         ... 11 more
      // Caused by: java.io.IOException: java.lang.InterruptedException
      //         at build.buildfarm.cas.cfc.CASFileCache.newOutput(CASFileCache.java:1110)
      //         at build.buildfarm.cas.cfc.CASFileCache.createUniqueWriteOutput(CASFileCache.java:1059)
      //         at build.buildfarm.cas.cfc.CASFileCache$4.getOutput(CASFileCache.java:999)
      //         at build.buildfarm.worker.shard.LocalCasWriter.insertStream(LocalCasWriter.java:70)
      //         ... 10 more
      // Caused by: java.lang.InterruptedException
      //         at build.buildfarm.cas.cfc.CASFileCache.charge(CASFileCache.java:2836)
      //         at build.buildfarm.cas.cfc.CASFileCache.putOrReferenceGuarded(CASFileCache.java:2856)
      //         at build.buildfarm.cas.cfc.CASFileCache.putOrReference(CASFileCache.java:2732)
      //         at build.buildfarm.cas.cfc.CASFileCache.putImpl(CASFileCache.java:2616)
      //         at build.buildfarm.cas.cfc.CASFileCache.newOutput(CASFileCache.java:1099)
      //         ... 13 more
      if (!write.isComplete()) {
        write.reset(); // we will not attempt retry with current behavior, abandon progress
        throw new IOException(Status.RESOURCE_EXHAUSTED.withCause(e).asRuntimeException());
      }
    }
  }
}
