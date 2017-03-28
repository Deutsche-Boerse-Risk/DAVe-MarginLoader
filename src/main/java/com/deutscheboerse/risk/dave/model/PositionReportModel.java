package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

@DataObject
public class PositionReportModel extends AbstractModel {

    public PositionReportModel(JsonObject json) {
        this.mergeIn(json);
    }

    public PositionReportModel(PrismaReports.PrismaHeader header, PrismaReports.PositionReport data) {
        super(header);

        verify(data);

        PrismaReports.PositionReportKey key = data.getKey();
        put("clearer", key.getClearer());
        put("member", key.getMember());
        put("account", key.getAccount());
        put("liquidationGroup", key.getLiquidationGroup());
        put("liquidationGroupSplit", key.getLiquidationGroupSplit());
        put("product", key.getProduct());
        put("callPut", key.getCallPut());
        put("contractYear", key.getContractYear());
        put("contractMonth", key.getContractMonth());
        put("expiryDay", key.getExpiryDay());
        put("exercisePrice", key.getExercisePrice());
        put("version", key.getVersion());
        put("flexContractSymbol", key.getFlexContractSymbol());

        put("netQuantityLs", data.getNetQuantityLs());
        put("netQuantityEa", data.getNetQuantityEa());
        put("clearingCurrency", data.getClearingCurrency());
        put("mVar", data.getMVar());
        put("compVar", data.getCompVar());
        put("compCorrelationBreak", data.getCompCorrelationBreak());
        put("compCompressionError", data.getCompCompressionError());
        put("compLiquidityAddOn", data.getCompLiquidityAddOn());
        put("compLongOptionCredit", data.getCompLongOptionCredit());
        put("productCurrency", data.getProductCurrency());
        put("variationPremiumPayment", data.getVariationPremiumPayment());
        put("premiumMargin", data.getPremiumMargin());
        put("normalizedDelta", data.getNormalizedDelta());
        put("normalizedGamma", data.getNormalizedGamma());
        put("normalizedVega", data.getNormalizedVega());
        put("normalizedRho", data.getNormalizedRho());
        put("normalizedTheta", data.getNormalizedTheta());
        put("underlying", data.getUnderlying());
    }

    private void verify(PrismaReports.PositionReport data) {
        PrismaReports.PositionReportKey key = data.getKey();

        checkArgument(data.hasKey(), "Missing position report key in AMQP data");
        checkArgument(key.hasClearer(), "Missing clearer in AMQP data");
        checkArgument(key.hasMember(), "Missing member in AMQP data");
        checkArgument(key.hasAccount(), "Missing account in AMQP data");
        checkArgument(key.hasLiquidationGroup(), "Missing liquidation group in AMQP data");
        checkArgument(key.hasLiquidationGroupSplit(), "Missing liquidation group split in AMQP data");
        checkArgument(key.hasProduct(), "Missing product in AMQP data");
        checkArgument(key.hasCallPut(), "Missing call put in AMQP data");
        checkArgument(key.hasContractYear(), "Missing contract year in AMQP data");
        checkArgument(key.hasContractMonth(), "Missing contract month in AMQP data");
        checkArgument(key.hasExpiryDay(), "Missing expiry day in AMQP data");
        checkArgument(key.hasExercisePrice(), "Missing exercise price in AMQP data");
        checkArgument(key.hasVersion(), "Missing version in AMQP data");
        checkArgument(key.hasFlexContractSymbol(), "Missing flex contract symbol in AMQP data");

        checkArgument(data.hasNetQuantityLs(), "Missing net quantity ls in AMQP data");
        checkArgument(data.hasNetQuantityEa(), "Missing net quantity ea in AMQP data");
        checkArgument(data.hasClearingCurrency(), "Missing clearing currency in AMQP data");
        checkArgument(data.hasMVar(), "Missing m var in AMQP data");
        checkArgument(data.hasCompVar(), "Missing comp var in AMQP data");
        checkArgument(data.hasCompCorrelationBreak(), "Missing comp correlation break in AMQP data");
        checkArgument(data.hasCompCompressionError(), "Missing comp compression error in AMQP data");
        checkArgument(data.hasCompLiquidityAddOn(), "Missing comp liquidity add on in AMQP data");
        checkArgument(data.hasCompLongOptionCredit(), "Missing comp long option credit in AMQP data");
        checkArgument(data.hasProductCurrency(), "Missing product currency in AMQP data");
        checkArgument(data.hasVariationPremiumPayment(), "Missing variation premium payment in AMQP data");
        checkArgument(data.hasPremiumMargin(), "Missing premium margin in AMQP data");
        checkArgument(data.hasNormalizedDelta(), "Missing normalized delta in AMQP data");
        checkArgument(data.hasNormalizedGamma(), "Missing normalized gamma in AMQP data");
        checkArgument(data.hasNormalizedVega(), "Missing normalized vega in AMQP data");
        checkArgument(data.hasNormalizedRho(), "Missing normalized rho in AMQP data");
        checkArgument(data.hasNormalizedTheta(), "Missing normalized theta in AMQP data");
        checkArgument(data.hasUnderlying(), "Missing underlying in AMQP data");
    }
}
