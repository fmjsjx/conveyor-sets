package com.github.fmjsjx.conveyor.util;

import java.util.List;

public class CollectionUtil {

    public static final boolean isEqual(List<?> a, List<?> b) {
        if (a.size() == b.size()) {
            var i = a.iterator();
            var j = b.iterator();
            for (; i.hasNext();) {
                var a0 = i.next();
                var b0 = j.next();
                if (!a0.equals(b0)) {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    private CollectionUtil() {
    }

}
