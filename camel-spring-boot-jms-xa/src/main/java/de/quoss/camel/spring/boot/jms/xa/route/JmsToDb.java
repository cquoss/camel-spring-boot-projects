package de.quoss.camel.spring.boot.jms.xa.route;

import de.quoss.narayana.helper.ConnectionFactoryProxy;
import de.quoss.narayana.helper.NarayanaTransactionHelper;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;
import org.apache.camel.builder.endpoint.EndpointRouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.apache.camel.component.sql.SqlComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.util.Assert;

import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

@Component
public class JmsToDb extends EndpointRouteBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmsToDb.class);

    private static final String SQL_INSERT_VALUE = "insert into account (#id, #name)";

    static final String ROUTE_ID = "jms-to-db";

    private final TransactionManager tm;

    private final PlatformTransactionManager ptm;

    public JmsToDb(final PlatformTransactionManager ptm, final TransactionManager tm) {
        final String methodName = "JmsToDb(PlatformTransactionManager, TransactionManager)";
        LOGGER.trace("{} start [tm={}]", methodName, tm);
        Assert.notNull(ptm, "Platform transaction manager must not be null.");
        this.ptm = ptm;
        Assert.notNull(tm, "Transaction manager must not be null.");
        this.tm = tm;
        LOGGER.debug("{} [tm.type={}]", methodName, tm.getClass().getCanonicalName());
        LOGGER.trace("{} end]", methodName);
    }

    @Override
    public void configure() {

        final String methodName = "configure()";

        ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory();

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
                })
                .to(sql(SQL_INSERT_VALUE));

    }

}
