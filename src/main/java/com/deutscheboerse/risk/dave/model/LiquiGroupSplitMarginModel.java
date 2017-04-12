package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

@DataObject
public class LiquiGroupSplitMarginModel extends AbstractModel {

    public LiquiGroupSplitMarginModel(JsonObject json) {
        this.mergeIn(json);
    }

    public LiquiGroupSplitMarginModel(PrismaReports.PrismaHeader header, PrismaReports.LiquiGroupSplitMargin data) {
        super(header);

        verify(data);

        PrismaReports.LiquiGroupSplitMarginKey key = data.getKey();
        put("clearer", key.getClearer());
        put("member", key.getMember());
        put("account", key.getAccount());
        put("liquidationGroup", key.getLiquidationGroup());
        put("liquidationGroupSplit", key.getLiquidationGroupSplit());
        put("marginCurrency", key.getMarginCurrency());

        put("premiumMargin", data.getPremiumMargin());
        put("marketRisk", data.getMarketRisk());
        put("liquRisk", data.getLiquRisk());
        put("longOptionCredit", data.getLongOptionCredit());
        put("variationPremiumPayment", data.getVariationPremiumPayment());
    }

    private void verify(PrismaReports.LiquiGroupSplitMargin data) {
        checkArgument(data.hasKey(), "Missing LGSM key in AMQP data");
        
        checkArgument(data.getKey().hasClearer(), "Missing LGSM clearer in AMQP data");
        checkArgument(data.getKey().hasMember(), "Missing LGSM member in AMQP data");
        checkArgument(data.getKey().hasAccount(), "Missing LGSM account in AMQP data");
        checkArgument(data.getKey().hasLiquidationGroup(), "Missing LGSM liquidation group in AMQP data");
        checkArgument(data.getKey().hasLiquidationGroupSplit(), "Missing LGSM liquidation group split in AMQP data");
        checkArgument(data.getKey().hasMarginCurrency(), "Missing LGSM margin currency in AMQP data");
        checkArgument(data.hasPremiumMargin(), "Missing LGSM premium margin in AMQP data");
        checkArgument(data.hasMarketRisk(), "Missing LGSM market risk in AMQP data");
        checkArgument(data.hasLiquRisk(), "Missing LGSM liqu risk in AMQP data");
        checkArgument(data.hasLongOptionCredit(), "Missing LGSM long option credit in AMQP data");
        checkArgument(data.hasVariationPremiumPayment(), "Missing LGSM variation premium payment in AMQP data");
    }
}
