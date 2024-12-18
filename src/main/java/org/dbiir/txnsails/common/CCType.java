package org.dbiir.txnsails.common;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;

public enum CCType {

    SER("SERIALIZABLE"),
    SER_TRANSITION("SERIALIZABLE_IN_TRANSITION"),
    SI_TAILOR("SI_TAILOR"),
    RC_TAILOR("RC_TAILOR"),
    DYNAMIC("DYNAMIC"),
    /* pure */
    RC("RC"),
    SI("SI"),
    NUM_CC("NUM_CC");
    private final String name;

    CCType(String name) {
        this.name = name;
    }

    protected static final Map<Integer, CCType> idx_lookup = new HashMap<>();
    protected static final Map<String, CCType> name_lookup = new HashMap<>();

    static {
        for (CCType vt : EnumSet.allOf(CCType.class)) {
            CCType.idx_lookup.put(vt.ordinal(), vt);
            CCType.name_lookup.put(vt.name().toUpperCase(), vt);
        }
    }

    public String getName() {
        return name;
    }

    public static CCType get(String name) {
        return (CCType.name_lookup.get(name.toUpperCase()));
    }
}
