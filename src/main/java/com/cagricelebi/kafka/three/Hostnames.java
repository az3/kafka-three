package com.cagricelebi.kafka.three;

import java.io.IOException;
import java.io.InputStream;
import java.util.Scanner;

/**
 * Use remotely.
 *
 * <pre>
 * 1. remove or comment out package declaration.
 * 2. javac Hostnames.java
 * 3. java Hostnames
 * </pre>
 *
 * @author cagricelebi
 */
public class Hostnames {

    public static void main(String[] args) {
        try {

            System.out.println("\n~~~ $ hostname: " + exec("hostname") + " ~~~\n\n");
            System.out.println("\n~~~ getHostNameWithLoopback: " + getHostNameWithLoopback() + " ~~~\n\n");
            System.out.println("java.net.InetAddress.getLocalHost().getHostName(): " + java.net.InetAddress.getLocalHost().getHostName());
            System.out.println("java.net.InetAddress.getLocalHost().getHostAddress(): " + java.net.InetAddress.getLocalHost().getHostAddress());
            System.out.println("java.net.InetAddress.getLocalHost().getCanonicalHostName(): " + java.net.InetAddress.getLocalHost().getCanonicalHostName());
            System.out.println("java.net.InetAddress.getLoopbackAddress().getHostName(): " + java.net.InetAddress.getLoopbackAddress().getHostName());
            System.out.println("java.net.InetAddress.getLoopbackAddress().getHostAddress(): " + java.net.InetAddress.getLoopbackAddress().getHostAddress());
            System.out.println("java.net.InetAddress.getLoopbackAddress().getCanonicalHostName(): " + java.net.InetAddress.getLoopbackAddress().getCanonicalHostName());
            java.util.Map<String, String> env = System.getenv();
            for (java.util.Map.Entry<String, String> entry : env.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                System.out.println("System.getenv(" + key + "): " + value);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     *
     * @param cmd One word cmd. "hostname"
     * @return null if empty
     * @throws IOException
     */
    public static String exec(String cmd) throws IOException {
        try (InputStream isis = Runtime.getRuntime().exec(cmd).getInputStream();
                Scanner s = new Scanner(isis).useDelimiter("\\A")) {
            String value2 = s.hasNext() ? s.next() : "";
            if (value2 != null) {
                value2 = value2.trim();
                if (!value2.isEmpty()) {
                    return value2;
                }
            }
        }
        return null;
    }

    public static String getHostNameWithLoopback() throws Exception {
        String candidate = java.net.InetAddress.getLocalHost().getHostName();
        if ((candidate.contains("localhost") || candidate.contains("localdomain"))
                || (candidate.startsWith("ec2") && candidate.contains("amazonaws"))) {
            java.util.Map<String, String> env = System.getenv();
            for (java.util.Map.Entry<String, String> entry : env.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();
                if (value != null && key.toLowerCase(java.util.Locale.ENGLISH).contentEquals("hostname")) {
                    candidate = value;
                }
            }
        }
        if (candidate.startsWith("ec2") && candidate.contains("amazonaws")) {
            String canonical = java.net.InetAddress.getLoopbackAddress().getCanonicalHostName();
            if (canonical != null && !canonical.contains("localhost") && !canonical.contains("localdomain")) {
                candidate = canonical;
            }
        }
        return candidate;
    }

}
