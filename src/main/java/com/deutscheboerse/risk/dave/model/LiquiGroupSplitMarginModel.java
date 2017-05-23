package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.grpc.LiquiGroupSplitMargin;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

@DataObject
public class LiquiGroupSplitMarginModel implements Model<LiquiGroupSplitMargin> {

    private final LiquiGroupSplitMargin grpc;

    public LiquiGroupSplitMarginModel(JsonObject json) {
        verifyJson(json);
        this.grpc = ((GrpcJsonWrapper)json).toGpb(LiquiGroupSplitMargin.class);
    }

    public LiquiGroupSplitMarginModel(PrismaReports.PrismaHeader header, PrismaReports.LiquiGroupSplitMargin data) {
        verifyPrismaHeader(header);
        verifyPrismaData(data);

        PrismaReports.LiquiGroupSplitMarginKey key = data.getKey();
        this.grpc = LiquiGroupSplitMargin.newBuilder()
                .setSnapshotId(header.getId())
                .setBusinessDate(header.getBusinessDate())
                .setTimestamp(header.getTimestamp())
                .setClearer(key.getClearer())
                .setMember(key.getMember())
                .setAccount(key.getAccount())
                .setLiquidationGroup(key.getLiquidationGroup())
                .setLiquidationGroupSplit(key.getLiquidationGroupSplit())
                .setMarginCurrency(key.getMarginCurrency())
                .setPremiumMargin(data.getPremiumMargin())
                .setMarketRisk(data.getMarketRisk())
                .setLiquRisk(data.getLiquRisk())
                .setLongOptionCredit(data.getLongOptionCredit())
                .setVariationPremiumPayment(data.getVariationPremiumPayment())
                .build();
    }

    @Override
    public JsonObject toJson() {
        return new GrpcJsonWrapper(this.grpc);
    }

    @Override
    public LiquiGroupSplitMargin toGrpc() {
        return this.grpc;
    }

    private void verifyPrismaData(PrismaReports.LiquiGroupSplitMargin data) {
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
