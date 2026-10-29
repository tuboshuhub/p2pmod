package dev.p2pmod.core;

import dev.p2pmod.support.Trace;

import javax.naming.Context;
import javax.naming.NamingException;
import javax.naming.directory.Attribute;
import javax.naming.directory.Attributes;
import javax.naming.directory.InitialDirContext;
import java.util.Hashtable;

public final class SrvResolver {

    private SrvResolver() {}

    public record Resolved(String host, int port) {}

    private static final String[] QUERIES = {
            "_p2p._udp.", "_minecraft._udp.", "_minecraft._tcp."
    };

    public static Resolved resolve(String domain) {
        if (domain == null || domain.isBlank()) return null;
        String d = domain.trim();
        if (d.endsWith(".")) d = d.substring(0, d.length() - 1);

        if (isIpLiteral(d)) return null;

        for (String prefix : QUERIES) {
            Resolved r = lookup(prefix + d);
            if (r != null) {
                Trace.info("[P2P] SRV {} -> {}:{}", prefix + d, r.host(), r.port());
                return r;
            }
        }
        Trace.debug("[P2P] {} has no SRV record, using the default port", d);
        return null;
    }

    private static Resolved lookup(String fqdn) {
        try {
            Hashtable<String, String> env = new Hashtable<>();
            env.put(Context.INITIAL_CONTEXT_FACTORY, "com.sun.jndi.dns.DnsContextFactory");
            env.put("com.sun.jndi.dns.timeout.initial", "2000");
            env.put("com.sun.jndi.dns.timeout.retries", "1");
            @SuppressWarnings("unchecked")
            Hashtable<String, String> ctxEnv = (Hashtable<String, String>) env;
            Attributes attrs = new InitialDirContext(ctxEnv).getAttributes(fqdn, new String[]{"SRV"});
            Attribute a = attrs.get("SRV");
            if (a == null || a.size() == 0) return null;
            return best(a);
        } catch (NamingException | RuntimeException e) {
            return null;
        }
    }

    private static Resolved best(Attribute a) throws NamingException {
        int bestPriority = Integer.MAX_VALUE;
        String bestTarget = null;
        int bestPort = -1;
        for (int i = 0; i < a.size(); i++) {
            String v = String.valueOf(a.get(i));
            if (v == null) continue;
            String[] parts = v.trim().split("\\s+");
            if (parts.length < 4) continue;
            try {
                int priority = Integer.parseInt(parts[0]);
                int port = Integer.parseInt(parts[2]);
                String target = parts[3];
                if (target.endsWith(".")) target = target.substring(0, target.length() - 1);
                if (target.isEmpty()) continue;
                if (priority < bestPriority) {
                    bestPriority = priority;
                    bestTarget = target;
                    bestPort = port;
                }
            } catch (NumberFormatException ignored) {
            }
        }
        return (bestTarget == null || bestPort <= 0) ? null : new Resolved(bestTarget, bestPort);
    }

    private static boolean isIpLiteral(String s) {
        if (s.indexOf(':') >= 0) return true;
        if (s.chars().allMatch(c -> c == '.' || (c >= '0' && c <= '9'))) {
            return s.chars().filter(c -> c == '.').count() == 3;
        }
        return false;
    }
}
