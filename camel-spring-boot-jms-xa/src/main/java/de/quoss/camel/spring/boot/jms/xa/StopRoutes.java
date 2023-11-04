package de.quoss.camel.spring.boot.jms.xa;

import de.quoss.camel.spring.boot.jms.xa.exception.JmsXaException;
import de.quoss.camel.spring.boot.jms.xa.route.JmsToDb;
import org.apache.camel.CamelContext;
import org.apache.camel.Route;
import org.apache.camel.spi.RouteController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.Assert;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@EnableScheduling
@Component
public class StopRoutes {

    private static final Logger LOGGER = LoggerFactory.getLogger(StopRoutes.class);

    private static final String[] ROUTES = { "jms-to-db" };

    private static final ExecutorService SERVICE = Executors.newFixedThreadPool(10);

    private boolean doStop = false;

    private final CamelContext ctx;

    public StopRoutes(final CamelContext ctx) {
        Assert.notNull(ctx, "Camel context must not be null.");
        this.ctx = ctx;
    }

    @Scheduled(fixedRate = 5000L)
    private void checkStopRoutes() {
        final String methodName = "checkStopRoutes";
        LOGGER.trace("{} start [doStop={}]", methodName, doStop);
        Map<String, Future<Boolean>> futures = new LinkedHashMap<>();
        if (doStop) {
            LOGGER.info("Stopping all configured routes:");
            for (final String routeId : ROUTES) {
                futures.put(routeId, SERVICE.submit(new StopRoute(ctx, routeId)));
                LOGGER.info("Route {} set up for stopping.", routeId);
            }
            LOGGER.info("Status of configured routes after trying to stop:");
            for (Map.Entry<String, Future<Boolean>> entry : futures.entrySet()) {
                try {
                    LOGGER.info("Route {}: {}", entry.getKey(), entry.getValue().get());
                } catch (ExecutionException e) {
                    LOGGER.error(String.format("Error waiting on route %s to stop.", entry.getKey()), e);
                } catch (InterruptedException e) {
                    LOGGER.error(String.format("Interrupted waiting on route %s to stop.", entry.getKey()), e);
                    Thread.currentThread().interrupt();
                }
            }
            doStop = false;
        }
    }

    public void setDoStop(final boolean value) {
        doStop = value;
    }


    private static class StopRoute implements Callable<Boolean> {

        private final CamelContext ctx;

        private final String routeId;

        StopRoute(final CamelContext ctx, final String routeId) {
            this.ctx = ctx;
            this.routeId = routeId;
        }

        @Override
        public Boolean call() throws InterruptedException {
            try {
                ctx.getRouteController().stopRoute(routeId);
            } catch (Exception e) {
              LOGGER.error("Error stopping route " + routeId + ".", e);
              return false;
            }
            while (!ctx.getRouteController().getRouteStatus(routeId).isStopped()) {
                Thread.sleep(1000L);
            }
            return true;
        }

    }


}
