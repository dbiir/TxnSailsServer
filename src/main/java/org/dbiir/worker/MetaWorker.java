package org.dbiir.worker;

import lombok.Getter;
import org.dbiir.analysis.ChordAbsentCycleFinder;
import org.dbiir.common.DependencyType;
import org.dbiir.common.StaticDependencyGraph;
import org.dbiir.common.StaticDependencyGraphEdge;
import org.dbiir.common.TransactionTemplate;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MetaWorker {
    @Getter
    private static MetaWorker INSTANCE = new MetaWorker();
    HashMap<String, TransactionTemplate> templates = new HashMap<>();
    StaticDependencyGraph sdg = new StaticDependencyGraph();

    public void registerTemplateName(String template_name) {
        templates.computeIfAbsent(template_name, k -> new TransactionTemplate(template_name));
    }

    public int registerTemplateSQL(String template_name, int op, String relation, String sql) {
        templates.computeIfAbsent(template_name, k -> new TransactionTemplate(template_name));
        return templates.get(template_name).addTemplateSQL(op, relation, sql);
    }

    public int registerTemplateSQL(String template_name, int op, String relation, String sql, int idx) {
        templates.computeIfAbsent(template_name, k -> new TransactionTemplate(template_name));
        templates.get(template_name).setSkipIndex(idx, true);
        return templates.get(template_name).addTemplateSQL(op, relation, sql);
    }

    /**
     * analysis the workload:
     *  - find the anomalies under SI and RC along with corresponding sql and templates
     */
    public void analysisWorkload() {
        // construct the static dependency graph
        this.sdg = new StaticDependencyGraph();
        for (Map.Entry<String, TransactionTemplate> entry: templates.entrySet()) {
            sdg.addTemplate(entry.getValue());
        }
        sdg.addAllDependenciesByTemplates();

        ChordAbsentCycleFinder finder = new ChordAbsentCycleFinder(sdg.getAdjacencyList());
        List<List<StaticDependencyGraphEdge>> allCycles = finder.findChordAbsentCycles();

        // analysis RC
        List<List<StaticDependencyGraphEdge>> cyclesWithReadWrite = findCyclesWithReadWrite(allCycles);

        // analysis SI
        List<List<StaticDependencyGraphEdge>> cyclesWithTwoConsecutiveReadWrite = findCyclesWithTwoConsecutiveReadWrite(allCycles);

        // output the result
        System.out.println("RC:");
        for (List<StaticDependencyGraphEdge> cycle : cyclesWithReadWrite) {
            System.out.println(cycle);
        }

        System.out.println("SI:");
        for (List<StaticDependencyGraphEdge> cycle : cyclesWithTwoConsecutiveReadWrite) {
            System.out.println(cycle);
        }
    }


    private static List<List<StaticDependencyGraphEdge>> findCyclesWithReadWrite(List<List<StaticDependencyGraphEdge>> allCycles) {
        List<List<StaticDependencyGraphEdge>> result = new ArrayList<>();
        for (List<StaticDependencyGraphEdge> cycle : allCycles) {
            for (StaticDependencyGraphEdge edge : cycle) {
                if (edge.getType() == DependencyType.READ_WRITE) {
                    result.add(cycle);
                    break;
                }
            }
        }
        return result;
    }

    private static List<List<StaticDependencyGraphEdge>> findCyclesWithTwoConsecutiveReadWrite(List<List<StaticDependencyGraphEdge>> allCycles) {
        List<List<StaticDependencyGraphEdge>> result = new ArrayList<>();
        for (List<StaticDependencyGraphEdge> cycle : allCycles) {
            for (int i = 0; i < cycle.size(); i++) {
                StaticDependencyGraphEdge edge1 = cycle.get(i);
                StaticDependencyGraphEdge edge2 = cycle.get((i + 1) % cycle.size());
                if (edge1.getType() == DependencyType.READ_WRITE && edge2.getType() == DependencyType.READ_WRITE) {
                    result.add(cycle);
                    break;
                }
            }
        }
        return result;
    }

    public String getSQLByIndex(String name, int idx) {
        if (templates.get(name) == null) {
            return "Invalid";
        }
        return templates.get(name).getSQLByIndex(idx);
    }

    public void flushAnalysisResult() {
        // failure recovery in extended work
    }
}
