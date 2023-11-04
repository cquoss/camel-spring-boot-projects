package de.quoss.camel.spring.boot.jms.xa.route;

import de.quoss.camel.spring.boot.jms.xa.exception.JmsXaException;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.camel.CamelContext;
import org.apache.camel.Processor;
import org.apache.camel.builder.AdviceWith;
import org.awaitility.Awaitility;
import org.awaitility.core.ConditionTimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jms.core.JmsTemplate;

import javax.jms.ConnectionFactory;
import javax.jms.Destination;
import javax.jms.JMSConsumer;
import javax.jms.JMSContext;
import javax.jms.JMSProducer;
import javax.jms.Topic;
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

    private static final String SQL_FULL_SELECT_ACCOUNT = "select * from account";

    private DbHelper dbHelper;

    @Autowired
    CamelContext ctx;

    // wire the db url here
    @Value("${spring.datasource.url}")
    String dbUrl;

    @BeforeEach
    void setUp() throws Exception {
        if (dbHelper == null) {
            dbHelper = new DbHelper(dbUrl);
        }
    }

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
        try {
            Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(new QueryDb(dbHelper, SQL_FULL_SELECT_ACCOUNT, 1));
        } catch (ConditionTimeoutException e) {
            Assertions.fail("Timed out while querying account table for one row.");
        }
    }

    /**
     * Test that message resides in DLQ when error in route occurs after retries are exhausted.
     * @throws Exception in case of unexpected test setup errors.
     */
    @Test
    void testJmsToDbError() throws Exception {
        final String methodName = "testJmsToDbError()";
        LOGGER.trace("{} start", methodName);
        Assertions.assertNotNull(ctx);
        LOGGER.debug("Adding processor producing on-purpose exception to the end of the route.");
        AdviceWith.adviceWith(ctx, JmsToDb.ROUTE_ID, a -> {
            a.weaveAddLast().process(e -> {
                throw new RuntimeException("This happens on purpose");
            }).id(JmsToDb.ROUTE_ID + ".process-error");
        });
        LOGGER.debug("Sending message to topic " + JmsToDb.JMS_TOPIC + ".");
        JMSContext context = CF.createContext();
        JMSProducer producer = context.createProducer();
        Topic topic = context.createTopic(JmsToDb.JMS_TOPIC);
        producer.send(topic, "0,test");
        LOGGER.debug("Waiting 200 millis for processing to kick in.");
        Thread.sleep(200L);
        LOGGER.debug("Waiting for message to appear in DLQ.");
        JMSConsumer consumer = context.createConsumer(context.createQueue("DLQ"));
        try {
            Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(new ReceiveMessage(consumer));
        } catch (ConditionTimeoutException e) {
            Assertions.fail("Timed out while waiting for message in DLQ.");
        }
        LOGGER.debug("Assert we have no record in result table.");
        Object sqlResult = dbHelper.executeSql(SQL_FULL_SELECT_ACCOUNT);
        Assertions.assertTrue(sqlResult instanceof List);
        List<Map<String, Object>> listResult = (List<Map<String, Object>>) sqlResult;
        Assertions.assertTrue(listResult.isEmpty());
        LOGGER.debug("Assert route is stopped.");
        try {
            Awaitility.waitAtMost(10, TimeUnit.SECONDS).until(new CheckRouteStopped(ctx, JmsToDb.ROUTE_ID));
        } catch (ConditionTimeoutException e) {
            Assertions.fail("Timed out while checking if route " + JmsToDb.ROUTE_ID + " is stopped.");
        }
        LOGGER.trace("{} end", methodName);
    }

    private static class ReceiveMessage implements Callable<Boolean> {

        ReceiveMessage(final JMSConsumer consumer) {
            this.consumer = consumer;
        }

        private final JMSConsumer consumer;

        public Boolean call() {
            return consumer.receive() != null;
        }

    }

    private static class QueryDb implements Callable<Boolean> {

        QueryDb(final DbHelper dbHelper, final String sql, final int rowCountExpected) {
            this.sql = sql;
            this.rowCountExpected = rowCountExpected;
            this.dbHelper = dbHelper;
        }

        private final DbHelper dbHelper;

        private final String sql;

        private final int rowCountExpected;

        public Boolean call() {
            try {
                Object result = dbHelper.executeSql(sql);
                if (result instanceof List) {
                    return ((List) result).size() == rowCountExpected;
                }
            } catch (SQLException e) {
                LOGGER.error("Error querying db for test.", e);
            }
            return false;
        }

    }

    private static class CheckRouteStopped implements Callable<Boolean> {

        private final CamelContext ctx;

        private final String routeId;

        CheckRouteStopped(final CamelContext ctx, final String routeId) {
            this.ctx = ctx;
            this.routeId = routeId;
        }

        public Boolean call() {
            try {
                return ctx.getRouteController().getRouteStatus(routeId).isStopped();
            } catch (Exception e) {
                LOGGER.error("Error checking if route " + routeId + "is stopped.", e);
            }
            return false;
        }

    }

    private static class DbHelper {

        public DbHelper(final String dbUrl) {
            if (dbUrl == null || dbUrl.isEmpty()) {
                throw new JmsXaException("Db url must not be null or empty.");
            }
            this.dbUrl = dbUrl;
        }

        private final String dbUrl;

        public Object executeSql(final String sql) throws SQLException  {
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

    @AfterEach
    void tearDown() throws Exception {
        // remove process endpoint from main route if configured
        List<Processor> l = ctx.getRoute(JmsToDb.ROUTE_ID).filter(JmsToDb.ROUTE_ID + ".process-error");
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
        // truncate account table
        dbHelper.executeSql("delete from account");
    }

}
