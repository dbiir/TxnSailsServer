package org.dbiir.analysis;
import org.dbiir.common.StaticDependencyGraphEdge;
import org.dbiir.common.TransactionTemplate;

import java.util.*;

public class ChordAbsentCycleFinder {
    private final Map<TransactionTemplate, List<StaticDependencyGraphEdge>> graph;
    private final List<List<StaticDependencyGraphEdge>> chordAbsentCycles;

    public ChordAbsentCycleFinder(Map<TransactionTemplate, List<StaticDependencyGraphEdge>> graph) {
        this.graph = graph;
        this.chordAbsentCycles = new ArrayList<>();
    }

    public List<List<StaticDependencyGraphEdge>> findChordAbsentCycles() {
        for (TransactionTemplate node : graph.keySet()) {
            findCycles(new ArrayList<>(), node, node, new HashSet<>(), new HashSet<>());
        }
        return chordAbsentCycles;
    }

    private void findCycles(List<StaticDependencyGraphEdge> path, TransactionTemplate start, TransactionTemplate current, Set<TransactionTemplate> visited, Set<String> attrs) {
        visited.add(current);

        for (StaticDependencyGraphEdge edge : graph.get(current)) {
            if (edge.getTo().equals(start) && path.size() > 2 && !hasChord(path)) {
                chordAbsentCycles.add(new ArrayList<>(path));
            } else if (!visited.contains(edge.getTo()) && !attrs.contains(edge.getEdgeName())) {
                path.add(edge);
                attrs.add(edge.getEdgeName());
                findCycles(path, start, edge.getTo(), visited, attrs);
                path.remove(path.size() - 1);
                attrs.remove(edge.getEdgeName());
            }
        }

        visited.remove(current);
    }

    private boolean hasChord(List<StaticDependencyGraphEdge> path) {
        Set<TransactionTemplate> nodes = new HashSet<>();
        for (StaticDependencyGraphEdge edge : path) {
            nodes.add(edge.getFrom());
            nodes.add(edge.getTo());
        }
        for (TransactionTemplate node : nodes) {
            for (StaticDependencyGraphEdge edge : graph.getOrDefault(node, Collections.emptyList())) {
                if (nodes.contains(edge.getTo()) && !isAdjacentInPath(edge, path)) {
                    return true;
                }
            }
        }
        return false;
    }

    private boolean isAdjacentInPath(StaticDependencyGraphEdge edge, List<StaticDependencyGraphEdge> path) {
        for (StaticDependencyGraphEdge currentEdge : path) {
            if ((currentEdge.getFrom().equals(edge.getFrom()) && currentEdge.getTo().equals(edge.getTo())) ||
                    (currentEdge.getFrom().equals(edge.getTo()) && currentEdge.getTo().equals(edge.getFrom()))) {
                return true;
            }
        }
        return false;
    }
}
