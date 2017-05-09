package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.PoolMargin;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

@DataObject
public class PoolMarginModel extends AbstractModel<PoolMargin> {

    public PoolMarginModel(JsonObject json) {
        this.mergeIn(json);
    }

    public PoolMarginModel(PrismaReports.PrismaHeader header, PrismaReports.PoolMargin data) {
        super(header);

        verify(data);

        PrismaReports.PoolMarginKey key = data.getKey();
        put("clearer", key.getClearer());
        put("pool", key.getPool());
        put("marginCurrency", key.getMarginCurrency());
        put("clrRptCurrency", data.getClrRptCurrency());
        put("requiredMargin", data.getRequiredMargin());
        put("cashCollateralAmount", data.getCashCollateralAmount());
        put("adjustedSecurities", data.getAdjustedSecurities());
        put("adjustedGuarantee", data.getAdjustedGuarantee());
        put("overUnderInMarginCurr", data.getOverUnderInMarginCurr());
        put("overUnderInClrRptCurr", data.getOverUnderInClrRptCurr());
        put("variPremInMarginCurr", data.getVariPremInMarginCurr());
        put("adjustedExchangeRate", data.getAdjustedExchangeRate());
        put("poolOwner", data.getPoolOwner());
    }

    @Override
    public PoolMargin toGrpc() {
        return PoolMargin.newBuilder()
                .setSnapshotId(this.getInteger("snapshotID"))
                .setBusinessDate(this.getInteger("businessDate"))
                .setTimestamp(this.getLong("timestamp"))
                .setClearer(this.getString("clearer"))
                .setPool(this.getString("pool"))
                .setMarginCurrency(this.getString("marginCurrency"))
                .setClrRptCurrency(this.getString("clrRptCurrency"))
                .setRequiredMargin(this.getDouble("requiredMargin"))
                .setCashCollateralAmount(this.getDouble("cashCollateralAmount"))
                .setAdjustedSecurities(this.getDouble("adjustedSecurities"))
                .setAdjustedGuarantee(this.getDouble("adjustedGuarantee"))
                .setOverUnderInMarginCurr(this.getDouble("overUnderInMarginCurr"))
                .setOverUnderInClrRptCurr(this.getDouble("overUnderInClrRptCurr"))
                .setVariPremInMarginCurr(this.getDouble("variPremInMarginCurr"))
                .setAdjustedExchangeRate(this.getDouble("adjustedExchangeRate"))
                .setPoolOwner(this.getString("poolOwner"))
                .build();
    }

    private void verify(PrismaReports.PoolMargin data) {
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
