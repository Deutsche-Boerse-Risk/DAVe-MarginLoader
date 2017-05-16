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
public class LiquiGroupSplitMarginModelTest {

    @Test
    public void testConversionFromPrisma() throws Exception {
        JsonObject json = DataHelper.getLastJsonFromFile("liquiGroupSplitMargin", 1).orElseThrow(Exception::new);
        PrismaReports.PrismaHeader header = DataHelper.createPrismaHeaderFromJson(json);
        PrismaReports.LiquiGroupSplitMargin data = DataHelper.createPrismaLiquiGroupSplitMarginFromJson(json);

        LiquiGroupSplitMarginModel modelFromPrisma = new LiquiGroupSplitMarginModel(header, data);
        LiquiGroupSplitMarginModel modelFromJson = DataHelper.createLiquiGroupSplitMarginModelFromJson(json);

        Assert.assertEquals(modelFromJson, modelFromPrisma);
    }

    @Test(expected = RuntimeException.class)
    public void testCorruptedLiquiGroupSplitMarginModel(TestContext context) {
        new LiquiGroupSplitMarginModel(new JsonObject().put("grpc", new byte[]{0}));
    }
}
