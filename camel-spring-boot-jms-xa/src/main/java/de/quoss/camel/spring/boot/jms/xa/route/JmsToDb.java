package de.quoss.camel.spring.boot.jms.xa.route;

import de.quoss.camel.spring.boot.jms.xa.exception.JmsXaException;
import de.quoss.narayana.helper.ConnectionFactoryProxy;
import de.quoss.narayana.helper.NarayanaTransactionHelper;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;
import org.apache.camel.Route;
import org.apache.camel.builder.endpoint.EndpointRouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.sql.SqlComponent;
import org.apache.camel.spi.RouteController;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.util.Assert;

import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;
import java.util.LinkedHashMap;
import java.util.Map;

@Component
public class JmsToDb extends EndpointRouteBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmsToDb.class);

    private static final String SQL_INSERT_VALUE = "insert into account (id, name) values (:#id, :#name)";

    static final String ROUTE_ID = "jms-to-db";

    private final TransactionManager tm;

    private final PlatformTransactionManager ptm;

    private final ActiveMQXAConnectionFactory cf;

    public JmsToDb(final PlatformTransactionManager ptm, final TransactionManager tm, final ActiveMQXAConnectionFactory cf) {
        final String methodName = "JmsToDb(PlatformTransactionManager, TransactionManager)";
        LOGGER.trace("{} start [tm={}]", methodName, tm);
        Assert.notNull(ptm, "Platform transaction manager must not be null.");
        this.ptm = ptm;
        Assert.notNull(tm, "Transaction manager must not be null.");
        this.tm = tm;
        LOGGER.debug("{} [tm.type={}]", methodName, tm.getClass().getCanonicalName());
        Assert.notNull(cf, "Connection factory must not be null.");
        this.cf = cf;
        LOGGER.trace("{} end]", methodName);
    }

    @Override
    public void configure() {

        final String methodName = "configure()";

        // stop all routes on any error
        // TODO check if stopping all routes can be achieved with less boilerplate code
        onException(Throwable.class)
                .process(e -> {
                    RouteController c = getContext().getRouteController();
                    for (Route r : getContext().getRoutes()) {
                        c.stopRoute(r.getRouteId());
                    }
                });

        final JmsComponent jmsComponent = ((JmsComponent) getContext().getComponent("jms"));
        jmsComponent.setTransactionManager(ptm);
        jmsComponent.setConnectionFactory(new ConnectionFactoryProxy(cf, new NarayanaTransactionHelper(tm)));
        jmsComponent.getConfiguration().setSynchronous(true);

        from(jms("topic:" + JmsToDb.ROUTE_ID)
                .durableSubscriptionName(JmsToDb.ROUTE_ID + "-shared-durable-subscription")
                .subscriptionShared(true)
                .advanced().receiveTimeout(1000L))
                .routeId(ROUTE_ID)
                .process(e -> {
                    Object body = e.getMessage().getBody();
                    LOGGER.info("{} [body.class.name={},body={}]", methodName, body.getClass().getName(), body);
                    String[] bodyParts = {};
                    if (body instanceof String) {
                        bodyParts = ((String) body).split(",");
                        if (bodyParts.length != 2) {
                            throw new JmsXaException("Message body invalid.");
                        }
                    }
                    final Map<String, String> dbColumn = new LinkedHashMap<>();
                    dbColumn.put("id", bodyParts[0]);
                    dbColumn.put("name", bodyParts[1]);
                    e.getMessage().setBody(dbColumn);
                })
                .to(sql(SQL_INSERT_VALUE)).id(JmsToDb.ROUTE_ID + ".sql-insert");

    }

}
