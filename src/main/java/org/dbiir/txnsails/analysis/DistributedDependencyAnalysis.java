package org.dbiir.txnsails.analysis;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Conservative vulnerable-read analysis for a closed, immutable distributed workload.
 *
 * <p>A vertex represents a complete transaction program, merging its possible branches. Operations
 * may be omitted, repeated, or reordered: no negative result depends on an operation being
 * mandatory or on its position in the program. Owner domains and program membership must be
 * enforced before business SQL executes, and every admitted program must appear in the same
 * catalogue at every owner. An unknown program cannot safely fall back independently while known
 * programs use these negatives.
 *
 * <p>Self dependencies represent different concurrent instances of the same program. Branch
 * identity is implicit in the merged vertex; only actual dependency edges contribute owner labels.
 * No WW edge suppresses an RW edge, and inverse WR alternatives remain in the overapproximation.
 */
public final class DistributedDependencyAnalysis {
  private DistributedDependencyAnalysis() {}

  public record AccessSite(
      String program, int slot, String table, boolean write, Set<Integer> owners) {
    public AccessSite {
      requireText(program, "program");
      requireSlot(slot);
      requireText(table, "table");
      table = table.toLowerCase(Locale.ROOT);
      if (owners == null || owners.isEmpty()) {
        throw new IllegalArgumentException("An access site needs at least one owner");
      }
      for (Integer owner : owners) requireOwner(owner);
      owners = Set.copyOf(owners);
    }
  }

  public record SiteOwner(String program, int slot, int owner) {
    public SiteOwner {
      requireText(program, "program");
      requireSlot(slot);
      requireOwner(owner);
    }
  }

  /** Masks are independent of the actual transaction scope; runtime certification still uses it. */
  public record Masks(boolean global, boolean rc, boolean si) {
    public boolean required(int jdbcIsolation) {
      boolean local =
          switch (jdbcIsolation) {
            case Connection.TRANSACTION_READ_COMMITTED -> rc;
            case Connection.TRANSACTION_REPEATABLE_READ -> si;
            case Connection.TRANSACTION_SERIALIZABLE -> false;
            default ->
                throw new IllegalArgumentException("Unsupported JDBC isolation " + jdbcIsolation);
          };
      return global || local;
    }
  }

  /** Returns an immutable entry for every read site and allowed owner, including false masks. */
  public static Map<SiteOwner, Masks> analyze(List<AccessSite> accessSites) {
    if (accessSites == null) throw new IllegalArgumentException("Access sites cannot be null");
    List<AccessSite> sites = new ArrayList<>(accessSites.size());
    Set<Slot> slots = new HashSet<>();
    Map<String, Integer> programs = new LinkedHashMap<>();
    Map<SiteOwner, Masks> masks = new HashMap<>();
    for (AccessSite site : accessSites) {
      if (site == null) throw new IllegalArgumentException("Access sites cannot contain null");
      if (!slots.add(new Slot(site.program(), site.slot()))) {
        throw new IllegalArgumentException(
            "Duplicate access slot " + site.program() + ":" + site.slot());
      }
      sites.add(site);
      programs.computeIfAbsent(site.program(), ignored -> programs.size());
      if (!site.write()) {
        for (int owner : site.owners()) {
          masks.put(
              new SiteOwner(site.program(), site.slot(), owner), new Masks(false, false, false));
        }
      }
    }

    List<Edge> edges = dependencies(sites, programs);
    // Every RW has an inverse WR, and every WW has an inverse WW. Therefore
    // connected components are exactly SCCs; union-find avoids recursive DFS.
    Components components = new Components(programs.size());
    for (Edge edge : edges) components.join(edge.from(), edge.to());

    Map<Integer, Set<Integer>> componentOwners = new HashMap<>();
    Set<ProgramOwner> incomingRw = new HashSet<>();
    for (Edge edge : edges) {
      int component = components.find(edge.from());
      if (component != components.find(edge.to())) continue;
      componentOwners.computeIfAbsent(component, ignored -> new HashSet<>()).add(edge.owner());
      if (edge.kind() == Kind.RW) {
        incomingRw.add(new ProgramOwner(edge.to(), edge.owner()));
      }
    }

    for (Edge edge : edges) {
      if (edge.kind() != Kind.RW) continue;
      int component = components.find(edge.from());
      if (component != components.find(edge.to())) continue;
      SiteOwner read = new SiteOwner(edge.reader().program(), edge.reader().slot(), edge.owner());
      Masks previous = masks.get(read);
      masks.put(
          read,
          new Masks(
              previous.global() || componentOwners.get(component).size() > 1,
              true,
              previous.si() || incomingRw.contains(new ProgramOwner(edge.from(), edge.owner()))));
    }
    return Map.copyOf(masks);
  }

  private static List<Edge> dependencies(List<AccessSite> sites, Map<String, Integer> programs) {
    List<Edge> edges = new ArrayList<>();
    for (int i = 0; i < sites.size(); i++) {
      AccessSite left = sites.get(i);
      for (int j = i; j < sites.size(); j++) {
        AccessSite right = sites.get(j);
        if ((!left.write() && !right.write()) || !left.table().equals(right.table())) continue;
        for (int owner : left.owners()) {
          if (!right.owners().contains(owner)) continue;
          int from = programs.get(left.program());
          int to = programs.get(right.program());
          if (left.write() && right.write()) {
            edges.add(new Edge(from, to, owner, Kind.WW, null));
            edges.add(new Edge(to, from, owner, Kind.WW, null));
          } else {
            AccessSite reader = left.write() ? right : left;
            int readerProgram = left.write() ? to : from;
            int writerProgram = left.write() ? from : to;
            edges.add(new Edge(readerProgram, writerProgram, owner, Kind.RW, reader));
            edges.add(new Edge(writerProgram, readerProgram, owner, Kind.WR, null));
          }
        }
      }
    }
    return edges;
  }

  private static void requireText(String value, String label) {
    if (value == null || value.isBlank()) {
      throw new IllegalArgumentException("Missing " + label);
    }
  }

  private static void requireSlot(int slot) {
    if (slot < 0) throw new IllegalArgumentException("Slot cannot be negative");
  }

  private static void requireOwner(Integer owner) {
    if (owner == null || owner < 0) throw new IllegalArgumentException("Invalid owner " + owner);
  }

  private enum Kind {
    RW,
    WR,
    WW
  }

  private record Slot(String program, int slot) {}

  private record ProgramOwner(int program, int owner) {}

  private record Edge(int from, int to, int owner, Kind kind, AccessSite reader) {}

  private static final class Components {
    private final int[] parent;
    private final byte[] rank;

    private Components(int count) {
      parent = new int[count];
      rank = new byte[count];
      for (int i = 0; i < count; i++) parent[i] = i;
    }

    private int find(int index) {
      while (parent[index] != index) {
        parent[index] = parent[parent[index]];
        index = parent[index];
      }
      return index;
    }

    private void join(int left, int right) {
      int a = find(left);
      int b = find(right);
      if (a == b) return;
      if (rank[a] < rank[b]) parent[a] = b;
      else {
        parent[b] = a;
        if (rank[a] == rank[b]) rank[a]++;
      }
    }
  }
}
