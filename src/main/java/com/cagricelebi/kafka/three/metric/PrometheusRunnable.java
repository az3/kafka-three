package com.cagricelebi.kafka.three.metric;

import io.prometheus.client.exporter.MetricsServlet;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.net.BindException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Starts a simple Jetty server and publishes /metrics on given port.
 * Based on: http://www.robustperception.io/instrumenting-java-with-prometheus/
 * https://github.com/RobustPerception/java_examples/blob/master/java_simple/src/main/java/io/robustperception/java_examples/JavaSimple.java
 *
 * @author cagricelebi
 */
public class PrometheusRunnable implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(PrometheusRunnable.class);

    private final String shutdownKey;
    private final int port;
    private Server server;

    public PrometheusRunnable(String shutdownKey, int port) {
        this.shutdownKey = shutdownKey;
        this.port = port;
    }

    @Override
    public void run() {
        try {
            server = new Server(port);
            ServletContextHandler context = new ServletContextHandler();
            context.setContextPath("/");
            server.setHandler(context);
            // Expose Prometheus metrics.
            context.addServlet(new ServletHolder(new MetricsServlet()), "/metrics");
            context.addServlet(new ServletHolder(new ShutdownServlet()), "/shutdown");
            // Add metrics about CPU, JVM memory etc.
            DefaultExports.initialize();
            // Start the webserver.
            server.start();
            server.join();
        } catch (BindException e) {
            // TODO Check "Address already in use" error.
            // Will we run multiple instances on the same machine?
            // Shall we make "port" as a parametric variable?
            logger.error(e.getMessage(), e);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
        }
    }

    private class ShutdownServlet extends HttpServlet {

        @Override
        protected void doGet(final HttpServletRequest req, final HttpServletResponse resp)
                throws ServletException, IOException {
            try {
                String kk = req.getParameter("key") != null ? req.getParameter("key") : "";
                if (kk.contentEquals(shutdownKey)) {
                    logger.info("Shutdown key matches '{}'.", shutdownKey);
                    new Thread() {
                        @Override
                        public void run() {
                            try {
                                server.stop();
                            } catch (Exception e1) {
                                logger.warn("Failed to stop Jetty.", e1);
                                try {
                                    logger.warn("Destroying Jetty.");
                                    server.destroy();
                                } catch (Exception e2) {
                                    logger.error("Failed to destroy Jetty.", e2);
                                }
                            }
                        }
                    }.start();
                } else {
                    logger.info("Shutdown key does not match '{}'.", shutdownKey);
                }
            } catch (Exception e) {
                logger.error(e.getMessage(), e);
            }

        }
    }

}
