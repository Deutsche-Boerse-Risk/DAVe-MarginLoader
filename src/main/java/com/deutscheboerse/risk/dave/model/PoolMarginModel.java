package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.grpc.PoolMargin;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

@DataObject
public class PoolMarginModel implements Model<PoolMargin> {

    private final PoolMargin grpc;

    public PoolMarginModel(JsonObject json) {
        verifyJson(json);
        this.grpc = json.mapTo(PoolMargin.class);
    }

    public PoolMarginModel(PrismaReports.PrismaHeader header, PrismaReports.PoolMargin data) {
        verifyPrismaHeader(header);
        verifyPrismaData(data);

        PrismaReports.PoolMarginKey key = data.getKey();
        this.grpc = PoolMargin.newBuilder()
                .setSnapshotId(header.getId())
                .setBusinessDate(header.getBusinessDate())
                .setTimestamp(header.getTimestamp())
                .setClearer(key.getClearer())
                .setPool(key.getPool())
                .setMarginCurrency(key.getMarginCurrency())
                .setClrRptCurrency(data.getClrRptCurrency())
                .setRequiredMargin(data.getRequiredMargin())
                .setCashCollateralAmount(data.getCashCollateralAmount())
                .setAdjustedSecurities(data.getAdjustedSecurities())
                .setAdjustedGuarantee(data.getAdjustedGuarantee())
                .setOverUnderInMarginCurr(data.getOverUnderInMarginCurr())
                .setOverUnderInClrRptCurr(data.getOverUnderInClrRptCurr())
                .setVariPremInMarginCurr(data.getVariPremInMarginCurr())
                .setAdjustedExchangeRate(data.getAdjustedExchangeRate())
                .setPoolOwner(data.getPoolOwner())
                .build();
    }

    @Override
    public PoolMargin toGrpc() {
        return this.grpc;
    }

    private void verifyPrismaData(PrismaReports.PoolMargin data) {
        checkArgument(data.hasKey(), "Missing pool key in AMQP data");
        checkArgument(data.getKey().hasClearer(), "Missing pool clearer in AMQP data");
        checkArgument(data.getKey().hasPool(), "Missing pool name in AMQP data");
        checkArgument(data.getKey().hasMarginCurrency(), "Missing pool margin currency in AMQP data");
        checkArgument(data.hasClrRptCurrency(), "Missing pool reporting currency in AMQP data");
        checkArgument(data.hasRequiredMargin(), "Missing pool required margin in AMQP data");
        checkArgument(data.hasCashCollateralAmount(), "Missing pool cash collateral amount in AMQP data");
        checkArgument(data.hasAdjustedSecurities(), "Missing pool adjusted securities in AMQP data");
        checkArgument(data.hasAdjustedGuarantee(), "Missing pool adjusted guarantee in AMQP data");
        checkArgument(data.hasOverUnderInMarginCurr(), "Missing pool over/under in margin currency in AMQP data");
        checkArgument(data.hasOverUnderInClrRptCurr(), "Missing pool over/under in reporting currency in AMQP data");
        checkArgument(data.hasVariPremInMarginCurr(), "Missing pool variation premium in AMQP data");
        checkArgument(data.hasAdjustedExchangeRate(), "Missing pool adjusted exchange rate in AMQP data");
        checkArgument(data.hasPoolOwner(), "Missing pool owner in AMQP data");
    }
}
