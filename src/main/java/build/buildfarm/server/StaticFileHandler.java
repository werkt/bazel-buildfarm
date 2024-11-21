package build.buildfarm.server;

import static io.netty.handler.codec.http.HttpResponseStatus.INTERNAL_SERVER_ERROR;
import static io.netty.handler.codec.http.HttpResponseStatus.NOT_FOUND;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_LENGTH;
import static io.netty.handler.codec.http.HttpHeaderNames.CONTENT_TYPE;

import com.google.common.io.ByteStreams;
import io.grpc.Metadata;
import io.grpc.netty.HttpHandler;
import io.grpc.netty.HttpResponseObserver;
import io.netty.handler.codec.http.HttpRequest;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import javax.activation.MimetypesFileTypeMap;

class StaticFileHandler implements HttpHandler {
  private static final MimetypesFileTypeMap mimeMap = new MimetypesFileTypeMap();

  private final Path root;

  StaticFileHandler(Path root) {
    this.root = root;
  }

  @Override
  public boolean handles(HttpRequest request, Metadata headers) {
    return true;
  }

  StaticFileHandler fileHandler(String path) {
    return new StaticFileHandler(root) {
      @Override
      public void handle(HttpRequest request, Metadata headers, HttpResponseObserver responseObserver) {
        handlePath(path, responseObserver);
      }
    };
  }

  void handlePath(String uriPath, HttpResponseObserver responseObserver) {
    try {
      Path path = root.resolve(uriPath.substring(1));
      long size = Files.size(path);
      if (size > Integer.MAX_VALUE) {
        throw new IOException("too big");
      }
      String contentType;
      if (uriPath.endsWith(".js")) {
        contentType = "application/x-javascript";
      } else {
        contentType = mimeMap.getContentType(uriPath);
      }
      responseObserver.headers().set(CONTENT_TYPE, contentType);
      responseObserver.headers().setInt(CONTENT_LENGTH, (int) size);
      responseObserver.onSuccess(Files.newInputStream(path));
    } catch (NoSuchFileException e) {
      // sends very little content currently...
      responseObserver.onError(NOT_FOUND);
    } catch (IOException e) {
      responseObserver.onError(INTERNAL_SERVER_ERROR);
    }
  }

  @Override
  public void handle(HttpRequest request, Metadata headers, HttpResponseObserver responseObserver) {
    // FIXME headers
    try {
      handlePath(new URI(request.uri()).getPath(), responseObserver);
    } catch (URISyntaxException e) {
      e.printStackTrace();
      responseObserver.onError(INTERNAL_SERVER_ERROR);
    }
  }
}
