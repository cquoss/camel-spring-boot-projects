package de.quoss.camel.spring.boot.jms.xa.route;

import de.quoss.camel.spring.boot.jms.xa.StopRoutes;
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

    static final String JMS_TOPIC = ROUTE_ID;
    static final String JMS_SUBSCRIPTION = ROUTE_ID + "-shared-durable-subscription";

    private final TransactionManager tm;

    private final PlatformTransactionManager ptm;

    private final ActiveMQXAConnectionFactory cf;

    private final StopRoutes stopRoutes;

    public JmsToDb(final PlatformTransactionManager ptm, final TransactionManager tm, final ActiveMQXAConnectionFactory cf,
                   final StopRoutes stopRoutes) {
        final String methodName = "JmsToDb(PlatformTransactionManager, TransactionManager)";
        LOGGER.trace("{} start [tm={}]", methodName, tm);
        Assert.notNull(ptm, "Platform transaction manager must not be null.");
        this.ptm = ptm;
        Assert.notNull(tm, "Transaction manager must not be null.");
        this.tm = tm;
        LOGGER.debug("{} [tm.type={}]", methodName, tm.getClass().getCanonicalName());
        Assert.notNull(cf, "Connection factory must not be null.");
        this.cf = cf;
        Assert.notNull(stopRoutes, "Stop routes bean must not be null.");
        this.stopRoutes = stopRoutes;
        LOGGER.trace("{} end]", methodName);
    }

    @Override
    public void configure() {

        final String methodName = "configure()";

        // stop all routes on any error using scheduled stop helper bean
        onException(Throwable.class)
                .process(e -> stopRoutes.setDoStop(true))
                .id(JmsToDb.ROUTE_ID + ".process-on-exception")
                .handled(false);

        final JmsComponent jmsComponent = ((JmsComponent) getContext().getComponent("jms"));
        jmsComponent.setTransactionManager(ptm);
        jmsComponent.setConnectionFactory(new ConnectionFactoryProxy(cf, new NarayanaTransactionHelper(tm)));
        jmsComponent.getConfiguration().setSynchronous(true);

        from(jms("topic:" + JmsToDb.ROUTE_ID)
                .durableSubscriptionName(JMS_SUBSCRIPTION)
                .subscriptionShared(true))
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
