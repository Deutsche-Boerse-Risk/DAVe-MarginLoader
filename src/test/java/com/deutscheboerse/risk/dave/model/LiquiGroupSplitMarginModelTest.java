package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.utils.DataHelper;
import io.vertx.core.json.JsonObject;
import org.junit.Assert;
import org.junit.Test;

public class LiquiGroupSplitMarginModelTest {

    @Test
    public void testGetAndSetValues() throws Exception {
        JsonObject json = DataHelper.getLastJsonFromFile("liquiGroupSplitMargin", 1).orElseThrow(Exception::new);
        PrismaReports.PrismaHeader header = DataHelper.createPrismaHeaderFromJson(json);
        PrismaReports.LiquiGroupSplitMargin data = DataHelper.createLiquiGroupSplitMarginGPBFromJson(json);

        LiquiGroupSplitMarginModel modelFromGPB = new LiquiGroupSplitMarginModel(header, data);
        LiquiGroupSplitMarginModel modelFromJson = new LiquiGroupSplitMarginModel(json);

        Assert.assertEquals(json, modelFromGPB.toJson());
        Assert.assertEquals(json, modelFromJson.toJson());
    }
}
