package no.uit.sfb.toolabstraction;

import com.beust.jcommander.JCommander;

// Workaround for bug
public class JCommanderFactory {
    public static JCommander get(Object obj) {
        return new JCommander(obj);
    }
}
