package org.dbiir.common;

import lombok.Getter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

public class StaticDependencyGraph {
    @Getter
    private HashMap<TransactionTemplate, List<StaticDependencyGraphEdge>> adjacencyList;
    private List<TransactionTemplate> templateList = new LinkedList<>();

    public StaticDependencyGraph() {
        this.adjacencyList = new HashMap<>();
    }

    public void addTemplate(TransactionTemplate template) {
        adjacencyList.putIfAbsent(template, new ArrayList<>());
    }

    public void addDependency(TransactionTemplate from, TransactionTemplate to, DependencyType type) {
        StaticDependencyGraphEdge edge = new StaticDependencyGraphEdge(from, to, type);
        // TODO: set edgeName
        adjacencyList.get(from).add(edge);
    }

    public void addDependency(TransactionTemplate from, TransactionTemplate to, DependencyType type, String edgeName) {
        StaticDependencyGraphEdge edge = new StaticDependencyGraphEdge(from, to, type);
        edge.setEdgeName(edgeName);
        adjacencyList.get(from).add(edge);
    }

    public List<StaticDependencyGraphEdge> getDependencies(TransactionTemplate template) {
        return adjacencyList.get(template);
    }

    public void addAllDependenciesByTemplates() {
        int listLength = templateList.size();
        List<TransactionTemplate> templateArrayList = new ArrayList<>();
        int idx = 0;
        for (TransactionTemplate t: templateList) {
            templateArrayList.add(t);
            idx++;
        }

        for (int i = 0; i < listLength; i++) {
            for (int j = i + 1; j < listLength; j++) {
                TransactionTemplate t1 = templateArrayList.get(i);
                TransactionTemplate t2 = templateArrayList.get(j);
                // find the dependencies
                addDependency(t1, t2);
            }
        }
    }

    private void addDependency(TransactionTemplate t1, TransactionTemplate t2) {
        int total_t1 = t1.totalSQLs();
        int total_t2 = t2.totalSQLs();
        for (int i = 0; i < total_t1; i++) {
            if (t1.shouldSkipIndex(i)) {
                continue;
            }
            for (int j = 0; j < total_t2; j++) {
                if (t2.shouldSkipIndex(j)) {
                    continue;
                }
                if (t1.getRelationByIndex(i).equals(t2.getRelationByIndex(j))) {
                    // 0 represents read, 1 represents write
                    if (t1.getOperationByIndex(i) == 0 && t2.getOperationByIndex(j) == 1) {
                        String edge_name = t1.getName() + "-" + t2.getName() + "-" + i + "-" + j;
                        addDependency(t1, t2, DependencyType.READ_WRITE, edge_name);
                        addDependency(t2, t1, DependencyType.WRITE_READ, edge_name);
                    } else if (t1.getOperationByIndex(i) == 1 && t2.getOperationByIndex(j) == 0) {
                        String edge_name = t2.getName() + "-" + t1.getName() + "-" + j + "-" + i;
                        addDependency(t1, t2, DependencyType.READ_WRITE, edge_name);
                        addDependency(t1, t2, DependencyType.WRITE_READ, edge_name);
                    } else if (t1.getOperationByIndex(i) == 0 && t2.getOperationByIndex(j) == 0) {
                        String edge_name = t1.getName() + "-" + t2.getName() + "-" + i + "-" + j;
                        addDependency(t1, t2, DependencyType.WRITE_WRITE, edge_name);
                        addDependency(t2, t1, DependencyType.WRITE_WRITE, edge_name);
                    }
                }
            }
        }
        return;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for (HashMap.Entry<TransactionTemplate, List<StaticDependencyGraphEdge>> entry : adjacencyList.entrySet()) {
            sb.append(entry.getKey().toString()).append(" -> ").append(entry.getValue().toString()).append("\n");
        }
        return sb.toString();
    }
}
