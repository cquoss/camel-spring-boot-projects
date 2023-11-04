package de.quoss.camel.spring.boot.jms.xa;

import org.apache.camel.CamelContext;
import org.apache.camel.CamelContextAware;
import org.apache.camel.Exchange;
import org.apache.camel.NamedNode;
import org.apache.camel.NamedRoute;
import org.apache.camel.Route;
import org.apache.camel.spi.ExchangeFormatter;
import org.apache.camel.spi.Tracer;
import org.apache.camel.support.CamelContextHelper;
import org.apache.camel.support.PatternHelper;
import org.apache.camel.support.builder.ExpressionBuilder;
import org.apache.camel.support.processor.DefaultExchangeFormatter;
import org.apache.camel.support.service.ServiceSupport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.util.Objects;
@Component
public class CustomTracer extends ServiceSupport implements CamelContextAware, Tracer {

    private static final String TRACING_OUTPUT = "%s [%s] [%s]";

    // use a fixed logger name so its easy to spot
    private static final Logger LOGGER = LoggerFactory.getLogger(CustomTracer.class);
    private CamelContext camelContext;
    private boolean enabled = true;
    private boolean standby;
    private long traceCounter;

    private ExchangeFormatter exchangeFormatter;
    private String tracePattern;
    private String[] patterns;
    private boolean traceBeforeAndAfterRoute = true;

    public CustomTracer() {
        DefaultExchangeFormatter formatter = new DefaultExchangeFormatter();
        // formatter.setShowExchangeId(true);
        // formatter.setShowExchangePattern(false);
        // formatter.setMultiline(false);
        // formatter.setShowHeaders(false);
        formatter.setStyle(DefaultExchangeFormatter.OutputStyle.Default);
        formatter.setShowAll(true);
        formatter.setMaxChars(0);
        setExchangeFormatter(formatter);
    }

    @Override
    public CamelContext getCamelContext() {
        return camelContext;
    }

    @Override
    public void setCamelContext(CamelContext camelContext) {
        this.camelContext = camelContext;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void traceBeforeNode(NamedNode node, Exchange exchange) {
        if (shouldTrace(node)) {
            traceCounter++;
            String routeId = ExpressionBuilder.routeIdExpression().evaluate(exchange, String.class);

            // we need to avoid leak the sensible information here
            // the sanitizeUri takes a very long time for very long string and the format cuts this to
            // 33 characters, anyway. Cut this to 50 characters. This will give enough space for removing
            // characters in the sanitizeUri method and will be reasonably fast
            // String label = URISupport.sanitizeUri(StringHelper.limitLength(node.getLabel(), 50));
            String label = node.getId();

            StringBuilder sb = new StringBuilder();
            sb.append(String.format(TRACING_OUTPUT, "   ", routeId, label));
            sb.append(" ");
            String data = exchangeFormatter.format(exchange);
            sb.append(data);
            String out = sb.toString();
            dumpTrace(out);
        }
    }

    @Override
    public void traceAfterNode(NamedNode node, Exchange exchange) {
        // noop
    }

    @Override
    public void traceAfterRoute(NamedRoute route, Exchange exchange) {
        // noop
    }

    @Override
    public void traceBeforeRoute(NamedRoute route, Exchange exchange) {
        if (!traceBeforeAndAfterRoute) {
            return;
        }

        // we need to avoid leak the sensible information here
        // the sanitizeUri takes a very long time for very long string and the format cuts this to
        // 33 characters, anyway. Cut this to 50 characters. This will give enough space for removing
        // characters in the sanitizeUri method and will be reasonably fast
        // String uri = route.getEndpointUrl();
        // String label = "from[" + URISupport.sanitizeUri(StringHelper.limitLength(uri, 50) + "]");
        String label = "from[" + route.getRouteId() + "]";

        // the arrow has a * if its a new exchange that is starting
        boolean original = route.getRouteId().equals(exchange.getFromRouteId());
        String arrow = original ? "*-->" : "--->";

        StringBuilder sb = new StringBuilder();
        sb.append(String.format(TRACING_OUTPUT, arrow, route.getRouteId(), label));
        sb.append(" ");
        String data = exchangeFormatter.format(exchange);
        sb.append(data);
        String out = sb.toString();
        dumpTrace(out);
    }

    @Override
    public void traceAfterRoute(Route route, Exchange exchange) {
        if (!traceBeforeAndAfterRoute) {
            return;
        }

        // we need to avoid leak the sensible information here
        // the sanitizeUri takes a very long time for very long string and the format cuts this to
        // 33 characters, anyway. Cut this to 50 characters. This will give enough space for removing
        // characters in the sanitizeUri method and will be reasonably fast
        // String uri = route.getConsumer().getEndpoint().getEndpointUri();
        // String label = "from[" + URISupport.sanitizeUri(StringHelper.limitLength(uri, 50) + "]");
        String label = "from[" + route.getId() + "]";

        // the arrow has a * if its an exchange that is done
        boolean original = route.getId().equals(exchange.getFromRouteId());
        String arrow = original ? "*<--" : "<---";

        StringBuilder sb = new StringBuilder();
        sb.append(String.format(TRACING_OUTPUT, arrow, route.getId(), label));
        sb.append(" ");
        String data = exchangeFormatter.format(exchange);
        sb.append(data);
        String out = sb.toString();
        dumpTrace(out);
    }

    @Override
    public boolean shouldTrace(NamedNode definition) {
        if (!enabled) {
            return false;
        }

        boolean pattern = true;

        if (patterns != null) {
            pattern = shouldTracePattern(definition);
        }

        LOGGER.trace("Should trace evaluated {} -> pattern: {}", definition.getId(), pattern);
        return pattern;
    }

    @Override
    public long getTraceCounter() {
        return traceCounter;
    }

    @Override
    public void resetTraceCounter() {
        traceCounter = 0;
    }

    @Override
    public boolean isEnabled() {
        return enabled;
    }

    @Override
    public void setEnabled(boolean enabled) {
        this.enabled = enabled;
    }

    @Override
    public boolean isStandby() {
        return standby;
    }

    @Override
    public void setStandby(boolean standby) {
        this.standby = standby;
    }

    @Override
    public String getTracePattern() {
        return tracePattern;
    }

    @Override
    public void setTracePattern(String tracePattern) {
        this.tracePattern = tracePattern;
        if (tracePattern != null) {
            // the pattern can have multiple nodes separated by comma
            this.patterns = tracePattern.split(",");
        } else {
            this.patterns = null;
        }
    }

    @Override
    public boolean isTraceBeforeAndAfterRoute() {
        return traceBeforeAndAfterRoute;
    }

    @Override
    public void setTraceBeforeAndAfterRoute(boolean traceBeforeAndAfterRoute) {
        this.traceBeforeAndAfterRoute = traceBeforeAndAfterRoute;
    }

    @Override
    public ExchangeFormatter getExchangeFormatter() {
        return exchangeFormatter;
    }

    @Override
    public void setExchangeFormatter(ExchangeFormatter exchangeFormatter) {
        this.exchangeFormatter = exchangeFormatter;
    }

    protected void dumpTrace(String out) {
        LOGGER.trace(out);
    }

    protected boolean shouldTracePattern(NamedNode definition) {
        for (String pattern : patterns) {
            // match either route id, or node id
            String id = definition.getId();
            // use matchPattern method from endpoint helper that has a good matcher we use in Camel
            if (PatternHelper.matchPattern(id, pattern)) {
                return true;
            }
            String routeId = CamelContextHelper.getRouteId(definition);
            if (routeId != null && !Objects.equals(routeId, id)) {
                if (PatternHelper.matchPattern(routeId, pattern)) {
                    return true;
                }
            }
        }
        // not matched the pattern
        return false;
    }

    @Override
    protected void doStart() throws Exception {
        // noop
    }

    @Override
    protected void doStop() throws Exception {
        // noop
    }
}
