package org.dbiir.txnsails.worker;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import lombok.Getter;
import lombok.Setter;
import org.dbiir.txnsails.analysis.ChordAbsentCycleFinder;
import org.dbiir.txnsails.analysis.SchemaInfo;
import org.dbiir.txnsails.common.*;
import org.dbiir.txnsails.common.constants.SmallBankConstants;
import org.dbiir.txnsails.common.constants.TPCCConstants;
import org.dbiir.txnsails.common.constants.YCSBConstants;
import org.dbiir.txnsails.common.types.DependencyType;

public class MetaWorker {
  @Getter private static MetaWorker INSTANCE = new MetaWorker();
  private HashMap<String, TransactionTemplate> templates = new HashMap<>();
  private StaticDependencyGraph sdg = new StaticDependencyGraph();
  private AtomicInteger globalTransactionGenerator;
  private int globalTemplateTypeGenerator;
  private final HashMap<String, Integer> relationEncodeMap = new HashMap<>();
  @Getter @Setter private SchemaInfo schema;

  public MetaWorker() {
    globalTemplateTypeGenerator = 1;
    registerTable();
  }

  public synchronized void registerTemplateName(String template_name) {
    templates.computeIfAbsent(
        template_name, k -> new TransactionTemplate(template_name, globalTemplateTypeGenerator));
    globalTemplateTypeGenerator <<= 1;
  }

  public int registerTemplateSQL(String template_name, int op, String relation, String sql) {
    templates.computeIfAbsent(template_name, k -> new TransactionTemplate(template_name));
    return templates.get(template_name).addTemplateSQL(op, relation, sql);
  }

  public int registerTemplateSQL(
      String template_name, int op, String relation, String sql, int idx) {
    templates.computeIfAbsent(template_name, k -> new TransactionTemplate(template_name));
    templates.get(template_name).setSkipIndex(idx, true);
    return templates.get(template_name).addTemplateSQL(op, relation, sql);
  }

  public int getRelationType(String relationName) {
    return this.relationEncodeMap.get(relationName);
  }

  private void registerTable() {
    this.relationEncodeMap.putAll(YCSBConstants.TABLENAME_TO_INDEX);
    this.relationEncodeMap.putAll(SmallBankConstants.TABLENAME_TO_INDEX);
    this.relationEncodeMap.putAll(TPCCConstants.TABLENAME_TO_INDEX);
  }

  /**
   * analysis the workload: - find the anomalies under SI and RC along with corresponding sql and
   * templates
   */
  public void analysisWorkload() {
    // construct the static dependency graph
    this.sdg = new StaticDependencyGraph();
    for (Map.Entry<String, TransactionTemplate> entry : templates.entrySet()) {
      sdg.addTemplate(entry.getValue());
    }
    sdg.addAllDependenciesByTemplates();

    ChordAbsentCycleFinder finder = new ChordAbsentCycleFinder(sdg.getAdjacencyList());
    System.out.println("============ find cycles ============");
    Set<StaticDependencyCycle> allCycles = finder.findAllCycles();
    for (StaticDependencyCycle cycle : allCycles) {
      System.out.print("Cycle: ");
      System.out.println(cycle);
    }

    // analysis RC
    System.out.println("============ find cycles ============");
    Set<StaticDependencyCycle> cyclesWithReadWrite = findCyclesWithReadWrite(allCycles);
    // output the result
    System.out.println("====== RC ======");
    for (StaticDependencyCycle cycle : cyclesWithReadWrite) {
      System.out.println(cycle);
    }
    for (StaticDependencyCycle cycle : cyclesWithReadWrite) {
      // Section 4.1, mark the needRewriteFlagUnderRC in template involved in RW dependency
      for (StaticDependencyGraphEdge edge : cycle.edges()) {
        if (edge.getType() == DependencyType.READ_WRITE) {
          edge.getFrom().setSQLRewriteByIndex(edge.getIdxInFrom(), "RC");
          edge.getTo().setSQLRewriteByIndex(edge.getIdxInTo(), "RC");
        }
      }
    }
    // analysis SI
    System.out.println("====== SI ======");
    Set<StaticDependencyCycle> cyclesWithTwoConsecutiveReadWrite =
        findCyclesWithTwoConsecutiveReadWrite(allCycles);
    for (StaticDependencyCycle cycle : cyclesWithTwoConsecutiveReadWrite) {
      System.out.println(cycle);
    }
    for (StaticDependencyCycle cycle : cyclesWithTwoConsecutiveReadWrite) {
      // Section 4.1, mark the needRewriteFlagUnderRC in template involved in second RW dependency
      List<StaticDependencyGraphEdge> edges = cycle.edges();
      for (int i = 0; i < edges.size(); i++) {
        if (edges.get(i).getType() == DependencyType.READ_WRITE
            && edges.get((i + 1) % edges.size()).getType() == DependencyType.READ_WRITE) {
          edges
              .get((i + 1) % edges.size())
              .getFrom()
              .setSQLRewriteByIndex(edges.get((i + 1) % edges.size()).getIdxInFrom(), "SI");
          edges
              .get((i + 1) % edges.size())
              .getTo()
              .setSQLRewriteByIndex(edges.get((i + 1) % edges.size()).getIdxInTo(), "SI");
        }
      }
    }
    System.out.println("========== find cycles end ==========");
  }

  private Set<StaticDependencyCycle> findCyclesWithReadWrite(Set<StaticDependencyCycle> allCycles) {
    Set<StaticDependencyCycle> result = new HashSet<>();

    for (StaticDependencyCycle cycle : allCycles) {
      for (StaticDependencyGraphEdge edge : cycle.edges()) {
        if (edge.getType() == DependencyType.READ_WRITE) {
          result.add(cycle);
          break; // find a cycle with one RW
        }
      }
    }
    return result;
  }

  private Set<StaticDependencyCycle> findCyclesWithTwoConsecutiveReadWrite(
      Set<StaticDependencyCycle> allCycles) {
    Set<StaticDependencyCycle> result = new HashSet<>();

    for (StaticDependencyCycle cycle : allCycles) {
      List<StaticDependencyGraphEdge> edges = cycle.edges();
      for (int i = 0; i < edges.size(); i++) {
        if (edges.get(i).getType() == DependencyType.READ_WRITE
            && edges.get((i + 1) % edges.size()).getType() == DependencyType.READ_WRITE) {
          result.add(cycle);
          break; // find a cycle with two consecutive RW
        }
      }
    }

    return result;
  }

  public void cloneTemplatesAfterAnalysis(HashMap<String, TransactionTemplate> cloneTemplates) {
    for (Map.Entry<String, TransactionTemplate> entry : templates.entrySet()) {
      cloneTemplates.put(entry.getKey(), entry.getValue().clone());
    }
  }

  public void flushAnalysisResult() {
    // for failure recovery
  }
}
