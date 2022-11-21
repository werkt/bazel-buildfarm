// Copyright 2021 The Bazel Authors. All rights reserved.
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

// This file was copied from the bazel project.
package build.buildfarm.common;

import build.buildfarm.common.io.FeedbackOutputStream;
import com.github.luben.zstd.ZstdInputStreamNoFinalizer;
import com.google.common.io.ByteStreams;
import com.google.protobuf.ByteString;
import java.io.ByteArrayInputStream;
import java.io.SequenceInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** An {@link OutputStream} that use zstd to decompress the content. */
public final class ZstdDecompressingOutputStream extends FeedbackOutputStream {
  private final OutputStream out;
  private InputStream inner = null;
  private final InputStream zis;

  public ZstdDecompressingOutputStream(OutputStream out) throws IOException {
    this.out = out;
    zis =
        new ZstdInputStreamNoFinalizer(
                new InputStream() {
                  @Override
                  public int read() throws IOException {
                    return inner.read();
                  }

                  @Override
                  public int read(byte[] b, int off, int len) throws IOException {
                    return inner.read(b, off, len);
                  }
                })
            .setContinuous(true);
  }

  @Override
  public void write(int b) throws IOException {
    write(new byte[] {(byte) b}, 0, 1);
  }

  @Override
  public void write(byte[] b) throws IOException {
    write(b, 0, b.length);
  }

  @Override
  public void write(byte[] b, int off, int len) throws IOException {
    InputStream compressed = new ByteArrayInputStream(b, off, len);
    if (inner != null) {
      inner = new SequenceInputStream(inner, compressed);
    } else {
      inner = compressed;
    }
    ByteStreams.copy(zis, out);
    if (inner.available() == 0) {
      inner = null;
    }
  }

  @Override
  public void close() throws IOException {
    closeShallow();
    out.close();
  }

  /**
   * Free resources related to decompression without closing the underlying {@link OutputStream}.
   */
  public void closeShallow() throws IOException {
    zis.close();
  }

  @Override
  public boolean isReady() {
    return true;
  }
}
