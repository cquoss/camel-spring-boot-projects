package de.quoss.camel.spring.boot.jms.xa.route;

import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.Processor;
import org.apache.camel.builder.AdviceWith;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

@SpringBootTest
class JmsToDbTest {

    private static final Logger LOGGER = LoggerFactory.getLogger(JmsToDbTest.class);

    // acquire connection factory (but not the auto-wired one from main route (duplicate client-id issue / xa issue)
    private static final ActiveMQConnectionFactory CF = new ActiveMQConnectionFactory("vm://localhost");

    @Autowired
    CamelContext ctx;

    // wire the db url here
    @Value("${spring.datasource.url}")
    String dbUrl;

    /**
     * Test to simply watch the xa pulse in log.
     *
     * @throws Exception in case of unexpected test setup errors.
     */
    @Test
    void testJmsToDbOk() throws Exception {
        // send message
        JmsTemplate template = new JmsTemplate(CF);
        template.setReceiveTimeout(1000L);
        Destination d = CF.createConnection().createSession().createTopic(JmsToDb.ROUTE_ID);
        template.convertAndSend(d, "0,test");
        // TODO replace thread sleep plus assertions using awaitility
        Thread.sleep(5500L);
        // assert we have a row in account table
        Object sqlResult = executeSql("select * from account");
        Assertions.assertTrue(sqlResult instanceof List);
        List<Map<String, Object>> listResult = (List<Map<String, Object>>) sqlResult;
        Assertions.assertEquals(1, listResult.size());
    }

    /**
     * Test that message resides in DLQ when error in route occurs after retries are exhausted.
     * @throws Exception in case of unexpected test setup errors.
     */
    @Test
    void testJmsToDbError() throws Exception {
        final String methodName = "testJmsToDbError()";
        LOGGER.trace("{} start", methodName);
        // acquire connection factory (but not the auto-wired one from main route (duplicate client-id issue / xa issue)
        ActiveMQConnectionFactory cf = new ActiveMQConnectionFactory("vm://localhost");
        // give set up some more time
        Thread.sleep(1000L);
        // stop the main route
        Assertions.assertNotNull(ctx);
        ctx.getRouteController().stopRoute(JmsToDb.ROUTE_ID);
        // add error producing processor before log node
        AdviceWith.adviceWith(ctx, JmsToDb.ROUTE_ID, a -> {
            a.weaveById(JmsToDb.ROUTE_ID + ".sql-insert").before().process(e -> {
                throw new RuntimeException("This happens on purpose");
            }).id(JmsToDb.ROUTE_ID + ".process-error");
        });
        LOGGER.debug("{} route weaved", methodName);
        // start the main route
        ctx.getRouteController().startRoute(JmsToDb.ROUTE_ID);
        // send message
        JmsTemplate template = new JmsTemplate(cf);
        template.setReceiveTimeout(1000L);
        Destination d = cf.createConnection().createSession().createTopic(JmsToDb.ROUTE_ID);
        template.convertAndSend(d, "0,test");
        LOGGER.debug("{} message sent", methodName);
        try {
            Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(new ReceiveMessage(cf));
        } catch (ConditionTimeoutException e) {
            Assertions.fail("Timed out while waiting for message to arrive in DLQ.");
        }
        // assert we have no row in account table
        Object sqlResult = executeSql("select * from account");
        Assertions.assertTrue(sqlResult instanceof List);
        List<Map<String, Object>> listResult = (List<Map<String, Object>>) sqlResult;
        Assertions.assertTrue(listResult.isEmpty());
        LOGGER.trace("{} end", methodName);
    }

    private static class ReceiveMessage implements Callable<Boolean> {

        ReceiveMessage(final ConnectionFactory cf) {
            this.cf = cf;
        }

        private ConnectionFactory cf;

        public Boolean call() {
            JmsTemplate template = new JmsTemplate(cf);
            Destination d = cf.createContext().createQueue("DLQ");
            return template.receive(d) != null;
        }

    }

    @AfterEach
    void tearDown() throws Exception {
        // remove process endpoint from main route if configured
        List<Processor> l = ctx.getRoute(JmsToDb.ROUTE_ID).filter(JmsToDb.ROUTE_ID + ".process");
        if (l.isEmpty()) {
            // do nothing
        } else {
            // stop the main route
            Assertions.assertNotNull(ctx);
            // remove error producing processor
            AdviceWith.adviceWith(ctx, JmsToDb.ROUTE_ID, a -> {
                a.weaveById(JmsToDb.ROUTE_ID + ".process-error").remove();
            });
            // start the main route
            ctx.getRouteController().startRoute(JmsToDb.ROUTE_ID);
        }
    }

    private Object executeSql(final String sql) throws SQLException  {
        Assertions.assertNotNull(dbUrl);
        Assertions.assertFalse(dbUrl.isEmpty());
        Object result = null;
        try (Connection c = DriverManager.getConnection(dbUrl, "sa", "");
             Statement s = c.createStatement()) {
            if (s.execute(sql)) {
                ResultSet rs = s.getResultSet();
                ResultSetMetaData rsm = rs.getMetaData();
                List<Map<String, Object>> table = new LinkedList<>();
                while (rs.next()) {
                    Map<String, Object> column = new LinkedHashMap<>();
                    for (int i = 1; i <= rsm.getColumnCount(); i++) {
                        column.put(rsm.getColumnName(i), rs.getObject(i));
                    }
                    table.add(column);
                }
                result = table;
            } else {
                result = s.getUpdateCount();
            }
        }
        return result;
    }

}
