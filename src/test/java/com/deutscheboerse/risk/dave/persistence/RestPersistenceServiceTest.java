package com.deutscheboerse.risk.dave.persistence;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import com.deutscheboerse.risk.dave.log.TestAppender;
import com.deutscheboerse.risk.dave.model.*;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import com.deutscheboerse.risk.dave.utils.TestConfig;
import io.vertx.core.AsyncResult;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.serviceproxy.ProxyHelper;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.stream.IntStream;

@RunWith(VertxUnitRunner.class)
public class RestPersistenceServiceTest {
    private static final TestAppender testAppender = TestAppender.getAppender(RestPersistenceService.class);
    private static final Logger rootLogger = (Logger) LoggerFactory.getLogger(Logger.ROOT_LOGGER_NAME);
    private static Vertx vertx;
    private static StoreManagerMock storageManager;
    private static PersistenceService persistenceProxy;

    @BeforeClass
    public static void setUp(TestContext context) throws IOException {
        RestPersistenceServiceTest.vertx = Vertx.vertx();

        JsonObject config = TestConfig.getStorageConfig();
        storageManager = new StoreManagerMock(vertx);
        storageManager.listen(context.asyncAssertSuccess());

        ProxyHelper.registerService(PersistenceService.class, vertx, new RestPersistenceService(vertx, config), PersistenceService.SERVICE_ADDRESS);
        RestPersistenceServiceTest.persistenceProxy = ProxyHelper.createProxy(PersistenceService.class, vertx, PersistenceService.SERVICE_ADDRESS);
        RestPersistenceServiceTest.persistenceProxy.initialize(context.asyncAssertSuccess());

        rootLogger.addAppender(testAppender);
    }

    @Test
    public void testAccountMarginStore(TestContext context) throws IOException {
        testStore(context, DataHelper.ACCOUNT_MARGIN_FOLDER, DataHelper::createAccountMarginModelFromJson,
                persistenceProxy::storeAccountMargin);
    }

    @Test
    public void testLiquiGroupMarginStore(TestContext context) throws IOException {
        testStore(context, DataHelper.LIQUI_GROUP_MARGIN_FOLDER, DataHelper::createLiquiGroupMarginModelFromJson,
                persistenceProxy::storeLiquiGroupMargin);
    }

    @Test
    public void testLiquiGroupSplitMarginStore(TestContext context) throws IOException {
        testStore(context, DataHelper.LIQUI_GROUP_SPLIT_MARGIN_FOLDER, DataHelper::createLiquiGroupSplitMarginModelFromJson,
                persistenceProxy::storeLiquiGroupSplitMargin);
    }

    @Test
    public void testPoolMarginStore(TestContext context) throws IOException {
        testStore(context, DataHelper.POOL_MARGIN_FOLDER, DataHelper::createPoolMarginModelFromJson,
                persistenceProxy::storePoolMargin);
    }

    @Test
    public void testPositionReportStore(TestContext context) throws IOException {
        testStore(context, DataHelper.POSITION_REPORT_FOLDER, DataHelper::createPositionReportModelFromJson,
                persistenceProxy::storePositionReport);
    }

    @Test
    public void testRiskLimitUtilizationStore(TestContext context) throws IOException {
        testStore(context, DataHelper.RISK_LIMIT_UTILIZATION_FOLDER, DataHelper::createRiskLimitUtilizationModelFromJson,
                persistenceProxy::storeRiskLimitUtilization);
    }

    @Test
    public void testStoreFailure(TestContext context) throws InterruptedException {
        storageManager.setHealth(false);
        testAppender.start();
        AccountMarginModel model = DataHelper.getLastModelFromFile(DataHelper.ACCOUNT_MARGIN_FOLDER, 1,
                DataHelper::createAccountMarginModelFromJson);
        persistenceProxy.storeAccountMargin(Collections.singletonList(model),
                context.asyncAssertFailure());
        testAppender.waitForMessageContains(Level.ERROR, "Service unavailable");
        testAppender.stop();
        storageManager.setHealth(true);
    }

    @Test
    public void testServiceUnavailableHandler(TestContext context) throws InterruptedException {
        Async closeAsync = context.async();
        storageManager.close(context.asyncAssertSuccess(i -> closeAsync.complete()));
        closeAsync.awaitSuccess();
        testAppender.start();
        AccountMarginModel model = DataHelper.getLastModelFromFile(DataHelper.ACCOUNT_MARGIN_FOLDER, 1,
                DataHelper::createAccountMarginModelFromJson);
        persistenceProxy.storeAccountMargin(Collections.singletonList(model),
                context.asyncAssertFailure());
        testAppender.waitForMessageContains(Level.ERROR, "Service unavailable");
        storageManager.listen(context.asyncAssertSuccess());
        testAppender.stop();
    }

    private static <T extends Model>
    void testStore(TestContext context, String dataFolder, Function<JsonObject, T> modelFactory,
                   BiConsumer<List<T>, Handler<AsyncResult<Void>>> sender) {

        IntStream.rangeClosed(1, 2).forEach(ttsaveNo -> {
            Async asyncStore = context.async(1);
            List<T> firstSnapshot = DataHelper.readTTSaveFile(dataFolder, ttsaveNo, modelFactory);
            sender.accept(firstSnapshot, ar -> {
                if (ar.succeeded()) {
                    asyncStore.countDown();
                } else {
                    context.fail(ar.cause());
                }
            });
            asyncStore.awaitSuccess(30000);
        });
    }

    @AfterClass
    public static void tearDown(TestContext context) {
        persistenceProxy.close();
        storageManager.close(context.asyncAssertSuccess());
        vertx.close(context.asyncAssertSuccess());
    }
}
