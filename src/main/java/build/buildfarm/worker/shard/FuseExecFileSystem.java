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

import build.bazel.remote.execution.v2.Action;
import build.bazel.remote.execution.v2.Command;
import build.bazel.remote.execution.v2.Compressor;
import build.bazel.remote.execution.v2.Digest;
import build.bazel.remote.execution.v2.Directory;
import build.buildfarm.cas.ContentAddressableStorage;
import build.buildfarm.worker.FuseCAS;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

class FuseExecFileSystem implements ExecFileSystem {
  private final Path root;
  private final FuseCAS fuseCAS;
  private final ContentAddressableStorage storage;

  FuseExecFileSystem(Path root, FuseCAS fuseCAS, ContentAddressableStorage storage) {
    this.root = root;
    this.fuseCAS = fuseCAS;
    this.storage = storage;
  }

  @Override
  public void start(Consumer<List<Digest>> onDigests, boolean skipLoad) {
    // onDigests.accept(storage.getAllDigests());
  }

  @Override
  public void stop() {
    fuseCAS.stop();
  }

  @Override
  public Path root() {
    return root;
  }

  @Override
  public ContentAddressableStorage getStorage() {
    return storage;
  }

  @Override
  public InputStream newInput(Compressor.Value compressor, Digest digest, long offset) throws IOException {
    return storage.newInput(compressor, digest, offset);
  }

  @Override
  public Path createExecDir(
      String operationName, Map<Digest, Directory> directoriesIndex, Action action, Command command)
      throws IOException, InterruptedException {
    fuseCAS.createInputRoot(operationName, action.getInputRootDigest());
    return root.resolve(operationName);
  }

  @Override
  public void destroyExecDir(Path actionRoot) throws IOException, InterruptedException {
    String topdir = root.relativize(actionRoot).toString();
    fuseCAS.destroyInputRoot(topdir);
  }
}
