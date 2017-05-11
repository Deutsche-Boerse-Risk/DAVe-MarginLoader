package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class PoolMarginModelTest {

    @Test
    public void testConversionFromPrisma() throws Exception {
        JsonObject json = DataHelper.getLastJsonFromFile("poolMargin", 1).orElseThrow(Exception::new);
        PrismaReports.PrismaHeader header = DataHelper.createPrismaHeaderFromJson(json);
        PrismaReports.PoolMargin data = DataHelper.createPrismaPoolMarginFromJson(json);

        PoolMarginModel modelFromPrisma = new PoolMarginModel(header, data);
        PoolMarginModel modelFromJson = DataHelper.createPoolMarginModelFromJson(json);

        Assert.assertEquals(modelFromJson, modelFromPrisma);
    }
}
