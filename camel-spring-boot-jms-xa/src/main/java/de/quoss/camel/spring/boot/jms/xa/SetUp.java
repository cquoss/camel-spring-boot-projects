package de.quoss.camel.spring.boot.jms.xa;

import de.quoss.camel.spring.boot.jms.xa.route.JmsToDb;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.jta.JtaTransactionManager;
import org.springframework.util.Assert;

import javax.transaction.TransactionManager;
import javax.transaction.UserTransaction;

public class SetUp {

    private static final Logger LOGGER = LoggerFactory.getLogger(SetUp.class);

    private final TransactionManager tm;

    private final UserTransaction ut;

    public SetUp(final TransactionManager tm, final UserTransaction ut) {
        final String methodName = "Main(TransactionManager, UserTransaction, ActiveMQXAConnectionFactory)";
        LOGGER.trace("{} start [tm={},ut={}]", methodName, tm, ut);
        Assert.notNull(tm, "Transaction manager must not be null.");
        this.tm = tm;
        LOGGER.debug("{} [tm.type={}]", methodName, tm.getClass().getCanonicalName());
        Assert.notNull(ut, "User transaction must not be null.");
        this.ut = ut;
        LOGGER.trace("{} end]", methodName);
    }

    @Bean
    public PlatformTransactionManager setupTm() {
        return new JtaTransactionManager(ut, tm);
    }

}
