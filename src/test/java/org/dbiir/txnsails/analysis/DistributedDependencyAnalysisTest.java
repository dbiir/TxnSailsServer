package org.dbiir.txnsails.analysis;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.dbiir.txnsails.analysis.DistributedDependencyAnalysis.AccessSite;
import org.dbiir.txnsails.analysis.DistributedDependencyAnalysis.Masks;
import org.dbiir.txnsails.analysis.DistributedDependencyAnalysis.SiteOwner;
import org.junit.jupiter.api.Test;

class DistributedDependencyAnalysisTest {
  @Test
  void globalFracturedSnapshotEdgesSpanActualDependencyOwners() {
    Map<SiteOwner, Masks> masks =
        analyze(
            r("reader", 0, "x", 0),
            r("reader", 1, "y", 1),
            w("writer", 0, "x", 0),
            w("writer", 1, "y", 1));
    assertEquals(new Masks(true, true, false), masks.get(key("reader", 0, 0)));
    assertEquals(new Masks(true, true, false), masks.get(key("reader", 1, 1)));
    assertTrue(masks.get(key("reader", 0, 0)).required(Connection.TRANSACTION_SERIALIZABLE));
  }

  @Test
  void unrelatedRemoteBranchDoesNotTurnLocalConflictIntoGlobalCycle() {
    Map<SiteOwner, Masks> masks =
        analyze(r("reader", 0, "x", 0), r("reader", 1, "uncontended", 1), w("writer", 0, "x", 0));
    assertEquals(new Masks(false, true, false), masks.get(key("reader", 0, 0)));
    assertEquals(new Masks(false, false, false), masks.get(key("reader", 1, 1)));
    assertTrue(masks.get(key("reader", 0, 0)).required(Connection.TRANSACTION_READ_COMMITTED));
    assertFalse(masks.get(key("reader", 0, 0)).required(Connection.TRANSACTION_REPEATABLE_READ));
    assertFalse(masks.get(key("reader", 0, 0)).required(Connection.TRANSACTION_SERIALIZABLE));
  }

  @Test
  void distinctInstancesOfOneTemplatePreserveSiWriteSkew() {
    Map<SiteOwner, Masks> masks =
        analyze(r("transfer", 0, "items", 0), w("transfer", 1, "items", 0));
    assertEquals(new Masks(false, true, true), masks.get(key("transfer", 0, 0)));
    assertTrue(masks.get(key("transfer", 0, 0)).required(Connection.TRANSACTION_REPEATABLE_READ));
  }

  @Test
  void globalStartingTransactionKeepsLocalSiSecondRwEdge() {
    Map<SiteOwner, Masks> masks =
        analyze(
            r("G1", 0, "x", 0), r("G1", 1, "w", 1),
            r("L1", 0, "y", 0), w("L1", 1, "x", 0),
            w("L2", 0, "y", 0), w("L2", 1, "z", 0),
            r("G2", 0, "z", 0), w("G2", 1, "w", 1));
    assertEquals(new Masks(true, true, true), masks.get(key("L1", 0, 0)));
    assertTrue(masks.get(key("G1", 0, 0)).global());
  }

  @Test
  void siPredecessorMustTouchThePivotAtTheSameOwnerAsItsRead() {
    Map<SiteOwner, Masks> masks =
        analyze(
            r("incoming", 0, "x", 1),
            w("pivot", 0, "x", 1),
            r("pivot", 1, "y", 0),
            w("outgoing", 0, "y", 0));
    assertFalse(masks.get(key("pivot", 1, 0)).si());
    assertTrue(masks.get(key("pivot", 1, 0)).global());
  }

  @Test
  void oneOwnerChainProtectsItsSecondRwWithoutRequiringGlobalMask() {
    Map<SiteOwner, Masks> masks =
        analyze(
            r("incoming", 0, "x", 0),
            w("pivot", 0, "x", 0),
            r("pivot", 1, "y", 0),
            w("outgoing", 0, "y", 0));
    assertEquals(new Masks(false, true, true), masks.get(key("pivot", 1, 0)));
    assertEquals(new Masks(false, true, false), masks.get(key("incoming", 0, 0)));
  }

  @Test
  void disjointOwnerDomainsAndUnwrittenTablesHaveFalseMasks() {
    Map<SiteOwner, Masks> masks =
        analyze(
            r("reader", 0, "items", 0),
            w("writer", 0, "ITEMS", 1),
            r("alone", 0, "unwritten", 0, 1));
    assertEquals(
        Set.of(key("reader", 0, 0), key("alone", 0, 0), key("alone", 0, 1)), masks.keySet());
    assertTrue(
        masks.values().stream().allMatch(mask -> mask.equals(new Masks(false, false, false))));
  }

  @Test
  void writeWriteEdgesStillJoinComponentsAndDoNotEraseReadWriteEdges() {
    Map<SiteOwner, Masks> masks =
        analyze(
            r("reader", 0, "x", 0),
            w("first", 0, "x", 0),
            w("first", 1, "shared", 1),
            w("second", 0, "shared", 1));
    assertTrue(masks.get(key("reader", 0, 0)).global());
    masks =
        analyze(
            r("first", 0, "items", 0),
            w("first", 1, "items", 0),
            r("second", 0, "items", 0),
            w("second", 1, "items", 0));
    assertTrue(masks.get(key("first", 0, 0)).rc());
    assertTrue(masks.get(key("first", 0, 0)).si());
    assertTrue(masks.get(key("second", 0, 0)).si());
  }

  @Test
  void separateComponentsDoNotShareOwnerLabels() {
    Map<SiteOwner, Masks> masks =
        analyze(
            r("readerA", 0, "x", 0),
            w("writerA", 0, "x", 0),
            r("readerB", 0, "y", 1),
            w("writerB", 0, "y", 1));
    assertFalse(masks.get(key("readerA", 0, 0)).global());
    assertFalse(masks.get(key("readerB", 0, 1)).global());
  }

  @Test
  void immutableSnapshotsAndCanonicalTableNamesMakeAnalysisStable() {
    Set<Integer> owners = new HashSet<>(Set.of(0));
    AccessSite read = new AccessSite("reader", 0, "ITEMS", false, owners);
    owners.add(1);
    assertEquals(Set.of(0), read.owners());
    assertEquals("items", read.table());
    assertThrows(UnsupportedOperationException.class, () -> read.owners().add(2));
    List<AccessSite> sites = new ArrayList<>(List.of(read, w("writer", 0, "items", 0)));
    Map<SiteOwner, Masks> masks = DistributedDependencyAnalysis.analyze(sites);
    sites.clear();
    assertTrue(masks.get(key("reader", 0, 0)).rc());
    assertThrows(
        UnsupportedOperationException.class,
        () -> masks.put(key("reader", 0, 0), new Masks(false, false, false)));
    assertEquals(Map.of(), DistributedDependencyAnalysis.analyze(List.of()));
  }

  @Test
  void malformedDescriptorsAndDuplicateSlotIdentitiesAreRejected() {
    assertThrows(
        IllegalArgumentException.class, () -> new AccessSite(" ", 0, "x", false, Set.of(0)));
    assertThrows(
        IllegalArgumentException.class, () -> new AccessSite("p", -1, "x", false, Set.of(0)));
    assertThrows(
        IllegalArgumentException.class, () -> new AccessSite("p", 0, " ", false, Set.of(0)));
    assertThrows(
        IllegalArgumentException.class, () -> new AccessSite("p", 0, "x", false, Set.of()));
    assertThrows(
        IllegalArgumentException.class, () -> new AccessSite("p", 0, "x", false, Set.of(-1)));
    assertThrows(IllegalArgumentException.class, () -> new AccessSite("p", 0, "x", false, null));
    assertThrows(IllegalArgumentException.class, () -> new SiteOwner("p", 0, -1));
    assertThrows(
        IllegalArgumentException.class, () -> analyze(r("p", 0, "x", 0), w("p", 0, "y", 1)));
    assertThrows(IllegalArgumentException.class, () -> DistributedDependencyAnalysis.analyze(null));
    assertThrows(
        IllegalArgumentException.class,
        () -> new Masks(false, false, false).required(Connection.TRANSACTION_READ_UNCOMMITTED));
  }

  private static Map<SiteOwner, Masks> analyze(AccessSite... sites) {
    return DistributedDependencyAnalysis.analyze(List.of(sites));
  }

  private static AccessSite r(String program, int slot, String table, Integer... owners) {
    return new AccessSite(program, slot, table, false, Set.of(owners));
  }

  private static AccessSite w(String program, int slot, String table, Integer... owners) {
    return new AccessSite(program, slot, table, true, Set.of(owners));
  }

  private static SiteOwner key(String program, int slot, int owner) {
    return new SiteOwner(program, slot, owner);
  }
}
