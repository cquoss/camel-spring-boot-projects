package de.quoss.camel.spring.boot.jms.xa.route;

import de.quoss.narayana.helper.ConnectionFactoryProxy;
import de.quoss.narayana.helper.NarayanaTransactionHelper;
import org.apache.activemq.artemis.jms.client.ActiveMQXAConnectionFactory;
import org.apache.camel.builder.endpoint.EndpointRouteBuilder;
import org.apache.camel.component.jms.JmsComponent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.util.Assert;

import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

@Component
public class JmsOk extends EndpointRouteBuilder {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmsOk.class);

    static final String ROUTE_ID = "jms-ok";

    private final TransactionManager tm;

    private final UserTransaction ut;

    public JmsOk(final TransactionManager tm, final UserTransaction ut) {
        final String methodName = "Main(TransactionManager, UserTransaction, ActiveMQXAConnectionFactory)";
        LOGGER.trace("{} start [tm={},ut={}]", methodName, tm, ut);
        Assert.notNull(tm, "Transaction manager must not be null.");
        this.tm = tm;
        LOGGER.debug("{} [tm.type={}]", methodName, tm.getClass().getCanonicalName());
        Assert.notNull(ut, "User transaction must not be null.");
        this.ut = ut;
        LOGGER.trace("{} end]", methodName);
    }

    @Override
    public void configure() {

        // TODO handle this hardcoded URL
        ActiveMQXAConnectionFactory cf = new ActiveMQXAConnectionFactory();

        final JmsComponent component = ((JmsComponent) getContext().getComponent("jms"));
        // cf.setClientID("client-id");
        component.setTransactionManager(new JtaTransactionManager(ut, tm));
        component.setConnectionFactory(new ConnectionFactoryProxy(cf, new NarayanaTransactionHelper(tm)));
        component.getConfiguration().setSynchronous(true);

        from(jms("topic:jms-ok").durableSubscriptionName("durable-subscription-ok").subscriptionShared(true).advanced().receiveTimeout(1000L))
                .routeId(ROUTE_ID)
                .log(ROUTE_ID).id(JmsOk.ROUTE_ID + ".log");

    }

}
