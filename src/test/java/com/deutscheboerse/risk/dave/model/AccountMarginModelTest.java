package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(VertxUnitRunner.class)
public class AccountMarginModelTest {

    @Test(expected = IllegalArgumentException.class)
    public void testCorruptedJson(TestContext context) {
        new AccountMarginModel(new JsonObject());
    }

    @Test
    public void testConversionFromPrisma() throws Exception {
        JsonObject json = DataHelper.getLastJsonFromFile("accountMargin", 1).orElseThrow(Exception::new);
        PrismaReports.PrismaHeader header = DataHelper.createPrismaHeaderFromJson(json);
        PrismaReports.AccountMargin data = DataHelper.createPrismaAccountMarginFromJson(json);

        AccountMarginModel modelFromPrisma = new AccountMarginModel(header, data);
        AccountMarginModel modelFromJson = DataHelper.createAccountMarginModelFromJson(json);

        Assert.assertEquals(modelFromJson, modelFromPrisma);
    }

    @Test(expected = RuntimeException.class)
    public void testCorruptedAccountMarginModel(TestContext context) {
        new AccountMarginModel(new JsonObject().put("grpc", new byte[]{0}));
    }
}
