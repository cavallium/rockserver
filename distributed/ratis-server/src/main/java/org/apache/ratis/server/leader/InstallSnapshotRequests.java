/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.server.leader;

import org.apache.ratis.proto.RaftProtos.FileChunkProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto;
import org.apache.ratis.proto.RaftProtos.InstallSnapshotRequestProto.SnapshotChunkProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RecoveryEligibilityStateMachine;
import org.apache.ratis.server.storage.FileChunkReader;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorageDirectory;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.util.JavaUtils;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.Objects;
import java.util.function.Function;

/**
 * An {@link Iterable} of {@link InstallSnapshotRequestProto} for sending a snapshot.
 * <p>
 * The snapshot is sent by one or more requests, where
 * a snapshot has one or more files, and
 * a file is sent by one or more chunks.
 * The number of requests is equal to the sum of the numbers of chunks of each file.
 */
class InstallSnapshotRequests implements Iterable<InstallSnapshotRequestProto> {
  private final RaftServer.Division server;
  private final RaftPeerId followerId;
  private final Function<FileInfo, Path> getRelativePath;
  private final RecoveryEligibilityStateMachine.Gate.SnapshotPermit snapshotPermit;

  /** The snapshot to be sent. */
  private final SnapshotInfo snapshot;
  /** A fixed id for all the requests. */
  private final String requestId;

  /** Maximum chunk size. */
  private final int snapshotChunkMaxSize;
  /** The total size of snapshot files. */
  private final long totalSize;
  /** The total number of snapshot files. */
  private final int numFiles;


  InstallSnapshotRequests(RaftServer.Division server, RaftPeerId followerId,
      String requestId, SnapshotInfo snapshot, int snapshotChunkMaxSize) {
    this(server, followerId, requestId, snapshot, snapshotChunkMaxSize, null, null);
  }

  static InstallSnapshotRequests source(RaftServer.Division server, RaftPeerId receiver,
      String requestId, SnapshotInfo snapshot, int chunkBytes, RecoveryEligibilityStateMachine.Gate.SnapshotPermit permit) {
    if (snapshot.getFiles().size() != 1 || snapshot.getTerm() < 1 || snapshot.getIndex() < 0) {
      throw new IllegalArgumentException("Source transfer requires one committed image");
    }
    final Path wirePath = new File(RaftStorageDirectory.STATE_MACHINE_DIR_NAME,
        org.apache.ratis.statemachine.impl.SimpleStateMachineStorage.getSnapshotFileName(
            snapshot.getTerm(), snapshot.getIndex())).toPath();
    return new InstallSnapshotRequests(server, receiver, requestId, snapshot, chunkBytes, ignored -> wirePath, permit);
  }

  private InstallSnapshotRequests(RaftServer.Division server, RaftPeerId followerId,
      String requestId, SnapshotInfo snapshot, int snapshotChunkMaxSize, Function<FileInfo, Path> sourcePath,
      RecoveryEligibilityStateMachine.Gate.SnapshotPermit suppliedPermit) {
    this.server = server;
    final var machine = server.getStateMachine();
    final var gate = machine instanceof RecoveryEligibilityStateMachine recovery
        ? Objects.requireNonNull(recovery.recoveryEligibilityGate()) : null;
    if (sourcePath != null) {
      if (gate == null && suppliedPermit != null) {
        throw new IllegalStateException("Nonparticipating snapshot source cannot use a recovery permit");
      }
      this.snapshotPermit = gate == null ? null : Objects.requireNonNull(suppliedPermit,
          "Qualified source artifact requires its recovery permit");
      if (snapshotPermit != null) { snapshotPermit.requireOwner(gate); }
    } else {
      this.snapshotPermit = gate == null ? null : gate.snapshotPermit();
      if (snapshotPermit != null) {
        requireLatestSnapshot(snapshot, machine.getLatestSnapshot());
        snapshotPermit.requireCurrent();
      }
    }
    this.followerId = followerId;
    this.requestId = requestId;
    this.snapshot = snapshot;
    this.snapshotChunkMaxSize = snapshotChunkMaxSize;
    final List<FileInfo> files = snapshot.getFiles();
    this.totalSize = files.stream().mapToLong(FileInfo::getFileSize).reduce(Long::sum).orElseThrow(
            () -> new IllegalStateException("Failed to compute total size for snapshot " + snapshot));
    this.numFiles = files.size();

    if (sourcePath != null) {
      this.getRelativePath = sourcePath;
      if (snapshotPermit != null) { snapshotPermit.requireCurrent(); }
      return;
    }

    final File snapshotDir = server.getStateMachine().getStateMachineStorage().getSnapshotDir();
    final Function<Path, Path> relativize;
    if (snapshotDir != null) {
      final Path dir = snapshotDir.toPath();
      relativize = p -> new File(RaftStorageDirectory.STATE_MACHINE_DIR_NAME, dir.relativize(p).toString()).toPath();
    } else {
      final Path dir = server.getRaftStorage().getStorageDir().getRoot().toPath();
      relativize = dir::relativize;
    }
    this.getRelativePath = info -> Optional.of(info.getPath())
        .filter(Path::isAbsolute)
        .map(relativize)
        .orElseGet(info::getPath);
    if (snapshotPermit != null) { snapshotPermit.requireCurrent(); }
  }

  @Override
  public RockserverSnapshotRequests.Requests iterator() {
    return new Iter();
  }

  private static void requireLatestSnapshot(SnapshotInfo supplied, SnapshotInfo latest) {
    if (latest == null || supplied.getTerm() != latest.getTerm() || supplied.getIndex() != latest.getIndex()) {
      throw new IllegalStateException("Snapshot source artifact differs from its cached latest snapshot");
    }
    final var expected = latest.getFiles();
    final var actual = supplied.getFiles();
    if (actual.size() != expected.size()) {
      throw new IllegalStateException("Snapshot source artifact file count changed");
    }
    for (int index = 0; index < actual.size(); index++) {
      final var file = actual.get(index);
      final var cached = expected.get(index);
      if (!file.getPath().equals(cached.getPath()) || file.getFileSize() != cached.getFileSize()
          || file.getFileDigest() == null || cached.getFileDigest() == null
          || !file.getFileDigest().equals(cached.getFileDigest())) {
        throw new IllegalStateException("Snapshot source artifact identity changed");
      }
    }
  }

  private class Iter implements RockserverSnapshotRequests.Requests {

    /** The index of the current request. */
    private int requestIndex = 0;
    /** The index of the current file. */
    private int fileIndex = 0;
    /** The current file. */
    private FileChunkReader current;
    private boolean closed;
    private boolean revoked;

    @Override public void close() throws IOException {
      if (closed) return;
      if (current != null) { current.close(); current = null; }
      closed = true;
    }

    @Override
    public boolean hasNext() {
      return !closed && !revoked && fileIndex < numFiles;
    }

    @Override
    public InstallSnapshotRequestProto next() {
      if (!hasNext()) {
        throw new NoSuchElementException("fileIndex = " + fileIndex + " >= numFiles = " + numFiles);
      }

      final FileInfo info = snapshot.getFiles().get(fileIndex);
      try {
        requireSnapshotPermit();
        if (current == null) {
          current = FileChunkReader.newInstance(info, getRelativePath.apply(info));
        }
        final FileChunkProto chunk = current.readFileChunk(snapshotChunkMaxSize);
        if (chunk.getDone()) {
          current.close();
          current = null;
          fileIndex++;
        }

        final boolean done = fileIndex == numFiles && chunk.getDone();
        return newInstallSnapshotRequest(chunk, done);
      } catch (IOException e) {
        if (current != null) {
          try {
            current.close();
            current = null;
          } catch (IOException ignored) {
          }
        }
        throw new IllegalStateException("Failed to iterate installSnapshot requests: " + this, e);
      }
    }

    private void requireSnapshotPermit() throws IOException {
      try {
        if (snapshotPermit != null) { snapshotPermit.requireCurrent(); }
      } catch (IllegalStateException stale) {
        revoked = true;
        throw new IOException("Native recovery state was revoked before snapshot production", stale);
      }
    }

    private InstallSnapshotRequestProto newInstallSnapshotRequest(FileChunkProto chunk, boolean done) throws IOException {
      synchronized (server) {
        requireSnapshotPermit();
        final SnapshotChunkProto.Builder b = LeaderProtoUtils.toSnapshotChunkProtoBuilder(
            requestId, requestIndex++, snapshot.getTermIndex(), chunk, totalSize, done);
        return LeaderProtoUtils.toInstallSnapshotRequestProto(server, followerId, b);
      }
    }
  }

  @Override
  public String toString() {
    return server.getId() + "->" + followerId + JavaUtils.getClassSimpleName(getClass())
        + ": requestId=" + requestId
        + ", snapshot=" + snapshot;
  }
}
