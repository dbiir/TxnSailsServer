package org.dbiir.worker;

import lombok.Getter;

public class OnlineWorker {
    @Getter
    private static OnlineWorker INSTANCE = new OnlineWorker();

    public  OnlineWorker() {

    }

    /**
     * online execution
     * @param args
     * @return execution results
     * args[0]: transaction template name
     * args[1]: sql index in this template
     */
    public String execute(String[] args) {
        if (args.length < 2)
            return "";

        String rewrite_sql = MetaWorker.getINSTANCE().getSQLByIndex(args[0], Integer.parseInt(args[1]));
        // TODO: execute the sql

        return rewrite_sql;
    }
}
