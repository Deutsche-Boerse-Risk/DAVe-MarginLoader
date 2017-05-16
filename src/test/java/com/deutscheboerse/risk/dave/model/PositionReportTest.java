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
public class PositionReportTest {

    @Test
    public void testConversionFromPrisma() throws Exception {
        JsonObject json = DataHelper.getLastJsonFromFile("positionReport", 1).orElseThrow(Exception::new);
        PrismaReports.PrismaHeader header = DataHelper.createPrismaHeaderFromJson(json);
        PrismaReports.PositionReport data = DataHelper.createPrismaPositionReportFromJson(json);

        PositionReportModel modelFromPrisma = new PositionReportModel(header, data);
        PositionReportModel modelFromJson = DataHelper.createPositionReportModelFromJson(json);

        Assert.assertEquals(modelFromJson, modelFromPrisma);
    }

    @Test(expected = RuntimeException.class)
    public void testCorruptedPositionReportModel(TestContext context) {
        new PositionReportModel(new JsonObject().put("grpc", new byte[]{0}));
    }
}
