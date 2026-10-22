package org.dbiir.txnsails.execution.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** Durable local sequences; the full coordinator ID travels separately in TransactionIdentity. */
public final class TransactionIdGenerator {
  private static final long BLOCK_SIZE = 4096;
  private static final long MAX_COUNTER = Long.MAX_VALUE;
  private static final AtomicLong EPHEMERAL = new AtomicLong(System.currentTimeMillis() << 10);
  private static final Map<Integer, Allocator> ALLOCATORS = new HashMap<>();

  private static class Allocator {
    final Path file;
    long next;
    long limit;

    Allocator(Path file) {
      this.file = file;
    }

    void reserve() throws IOException {
      Files.createDirectories(file.toAbsolutePath().getParent());
      try (FileChannel channel =
              FileChannel.open(
                  file,
                  StandardOpenOption.CREATE,
                  StandardOpenOption.READ,
                  StandardOpenOption.WRITE);
          var lock = channel.lock()) {
        long previous = 0;
        if (channel.size() != 0) {
          if (channel.size() != 16) throw new IOException("Invalid transaction ID checkpoint");
          ByteBuffer data = ByteBuffer.allocate(16);
          while (data.hasRemaining())
            if (channel.read(data) < 0)
              throw new IOException("Truncated transaction ID checkpoint");
          data.flip();
          previous = data.getLong();
          if (data.getLong() != ~previous || previous < 0 || previous > MAX_COUNTER) {
            throw new IOException("Corrupt transaction ID checkpoint");
          }
        }
        long candidateNext = Math.max(previous, System.currentTimeMillis() << 10);
        if (candidateNext > MAX_COUNTER - BLOCK_SIZE)
          throw new IOException("Transaction ID space exhausted");
        long candidateLimit = candidateNext + BLOCK_SIZE;
        ByteBuffer checkpoint =
            ByteBuffer.allocate(16).putLong(candidateLimit).putLong(~candidateLimit);
        checkpoint.flip();
        channel.position(0);
        while (checkpoint.hasRemaining()) channel.write(checkpoint);
        channel.force(true);
        // A failed checkpoint must never expose IDs from an unreserved block.
        next = candidateNext;
        limit = candidateLimit;
      }
    }
  }

  public static synchronized void configure(Path checkpoint, int instanceID) throws IOException {
    validateId(instanceID);
    Allocator allocator = new Allocator(checkpoint);
    allocator.reserve();
    ALLOCATORS.put(instanceID, allocator);
  }

  public static synchronized long generateTransactionId(int instanceID) {
    validateId(instanceID);
    Allocator allocator = ALLOCATORS.get(instanceID);
    long counter;
    if (allocator == null) {
      // Standalone callers/tests have no restart guarantee; distributed servers always configure.
      counter =
          EPHEMERAL.updateAndGet(
              previous -> Math.max(previous + 1, System.currentTimeMillis() << 10));
    } else {
      try {
        if (allocator.next == allocator.limit) allocator.reserve();
      } catch (IOException ex) {
        throw new IllegalStateException("Cannot reserve transaction IDs", ex);
      }
      counter = allocator.next++;
    }
    return counter;
  }

  private static void validateId(int id) {
    if (id < 0) throw new IllegalArgumentException("Instance ID must be nonnegative");
  }
}
