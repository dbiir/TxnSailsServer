package org.dbiir.worker;

import lombok.Getter;

public class OfflineWorker {
    @Getter
    public static OfflineWorker INSTANCE = new OfflineWorker();

    /**
     * start to register a transaction template
     * @param args
     * args[0]: template name
     */
    public void register_begin(String[] args) {
        if (args.length < 1)
            return;
        MetaWorker.getINSTANCE().registerTemplateName(args[0]);
    }

    /**
     * We analyze the read/write dependencies on the relation granularity offline for now,
     * which is sufficient for the benchmark used in our evaluation.
     * A more fine-grained analysis as our future work.
     * <p>
     * args[0]: transaction template name
     * args[1]: op name, Read or Write, Scan is included in Read
     * args[2]: table name, lowercase
     * args[3]: sql
     * args[4]: sql idx of previous read, write to the record
     */
    public int register(String[] args) {
        if (args.length < 4)
            return -1;
        if (args[1].equalsIgnoreCase("write") || args[1].equalsIgnoreCase("writes")) {
            if (args.length == 5)
                return MetaWorker.getINSTANCE().registerTemplateSQL(args[0], 1, args[2], args[3], Integer.parseInt(args[4]));
            else
                return MetaWorker.getINSTANCE().registerTemplateSQL(args[0], 1, args[2], args[3]);
        } else {
            if (args.length == 5)
                return MetaWorker.getINSTANCE().registerTemplateSQL(args[0], 0, args[2], args[3], Integer.parseInt(args[4]));
            else
                return MetaWorker.getINSTANCE().registerTemplateSQL(args[0], 0, args[2], args[3]);
        }
    }

    /**
     * register end
     * invoke the analysis and flush the analysis results in local disk
     */
    public void register_end(String[] args) {
        if (args.length < 1)
            return;
        MetaWorker.getINSTANCE().analysisWorkload();
        flush_analysis_results();
    }

    private void flush_analysis_results() {
        MetaWorker.getINSTANCE().flushAnalysisResult();
    }
}
