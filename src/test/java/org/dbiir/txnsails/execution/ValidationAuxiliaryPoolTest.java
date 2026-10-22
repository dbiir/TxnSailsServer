package org.dbiir.txnsails.execution;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.util.Collections;
import java.util.List;
import org.dbiir.txnsails.execution.validation.ValidationMetaTable;
import org.junit.jupiter.api.Test;

class ValidationAuxiliaryPoolTest {
  @Test
  void fixedNativeSerializableInitializesMetadataWithoutBenchmarkCaches() {
    var table = new ValidationMetaTable();
    var connections = Collections.nCopies(3, (Connection) null);
    table.initDistributed("old", List.of());
    table.initSingleSerializable("ycsb", connections);
    assertEquals("ycsb", table.getWorkload());
    assertSame(connections, table.connections);
    assertEquals(3, table.connectionGuards.size());
    // Fixed SER does not allocate the million-entry YCSB validation cache.
    assertEquals(
        "unknown table name: usertable",
        assertThrows(RuntimeException.class, () -> table.getHotspotVersion("usertable", 0))
            .getMessage());
    assertThrows(
        IllegalArgumentException.class, () -> table.initSingleSerializable("ycsb", List.of()));
  }

  @Test
  void hotspotInitializationUsesTheConfiguredPoolAndReplacesItsGuards() throws Exception {
    var table = new ValidationMetaTable();
    // An empty custom workload exercises pool setup without allocating benchmark row caches.
    table.initHotspot("empty", Collections.nCopies(3, (Connection) null));
    assertEquals(3, table.connectionGuards.size());
    var loadThreads = ValidationMetaTable.class.getDeclaredField("LOAD_THREAD");
    loadThreads.setAccessible(true);
    assertEquals(3, loadThreads.getInt(table));
    table.initHotspot("empty", Collections.singletonList(null));
    assertEquals(1, table.connectionGuards.size());
    assertEquals(1, loadThreads.getInt(table));
  }

  @Test
  void hotspotInitializationRejectsAnEmptyAuxiliaryPool() {
    assertThrows(
        IllegalArgumentException.class,
        () -> new ValidationMetaTable().initHotspot("empty", List.of()));
  }
}
