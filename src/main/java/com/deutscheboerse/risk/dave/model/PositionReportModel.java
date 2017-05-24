package com.deutscheboerse.risk.dave.model;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import com.deutscheboerse.risk.dave.grpc.PositionReport;
import io.vertx.codegen.annotations.DataObject;
import io.vertx.core.json.JsonObject;

import static com.google.common.base.Preconditions.checkArgument;

@DataObject
public class PositionReportModel implements Model<PositionReport> {

    private final PositionReport grpc;

    public PositionReportModel(JsonObject json) {
        verifyJson(json);
        this.grpc = json.mapTo(PositionReport.class);
    }

    public PositionReportModel(PrismaReports.PrismaHeader header, PrismaReports.PositionReport data) {
        verifyPrismaHeader(header);
        verifyPrismaData(data);

        PrismaReports.PositionReportKey key = data.getKey();
        this.grpc = PositionReport.newBuilder()
                .setSnapshotId(header.getId())
                .setBusinessDate(header.getBusinessDate())
                .setTimestamp(header.getTimestamp())
                .setClearer(key.getClearer())
                .setMember(key.getMember())
                .setAccount(key.getAccount())
                .setLiquidationGroup(key.getLiquidationGroup())
                .setLiquidationGroupSplit(key.getLiquidationGroupSplit())
                .setProduct(key.getProduct())
                .setCallPut(key.getCallPut())
                .setContractYear(key.getContractYear())
                .setContractMonth(key.getContractMonth())
                .setExpiryDay(key.getExpiryDay())
                .setExercisePrice(key.getExercisePrice())
                .setVersion(key.getVersion())
                .setFlexContractSymbol(key.getFlexContractSymbol())
                .setNetQuantityLs(data.getNetQuantityLs())
                .setNetQuantityEa(data.getNetQuantityEa())
                .setClearingCurrency(data.getClearingCurrency())
                .setMVar(data.getMVar())
                .setCompVar(data.getCompVar())
                .setCompCorrelationBreak(data.getCompCorrelationBreak())
                .setCompCompressionError(data.getCompCompressionError())
                .setCompLiquidityAddOn(data.getCompLiquidityAddOn())
                .setCompLongOptionCredit(data.getCompLongOptionCredit())
                .setProductCurrency(data.getProductCurrency())
                .setVariationPremiumPayment(data.getVariationPremiumPayment())
                .setPremiumMargin(data.getPremiumMargin())
                .setNormalizedDelta(data.getNormalizedDelta())
                .setNormalizedGamma(data.getNormalizedGamma())
                .setNormalizedVega(data.getNormalizedVega())
                .setNormalizedRho(data.getNormalizedRho())
                .setNormalizedTheta(data.getNormalizedTheta())
                .setUnderlying(data.getUnderlying())
                .build();
    }

    @Override
    public PositionReport toGrpc() {
        return this.grpc;
    }

    private void verifyPrismaData(PrismaReports.PositionReport data) {
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
