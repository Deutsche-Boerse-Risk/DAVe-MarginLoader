package com.deutscheboerse.risk.dave;

import CIL.CIL_v001.Prisma_v001.PrismaReports;
import CIL.ObjectList;
import com.deutscheboerse.risk.dave.model.ModelType;
import com.deutscheboerse.risk.dave.model.PositionReportModel;
import io.vertx.core.Future;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;

public class PositionReportVerticle extends AMQPVerticle {
    private static final Logger LOG = LoggerFactory.getLogger(PositionReportVerticle.class);

    @Override
    public void start(Future<Void> fut) throws Exception {
        super.start(fut, PositionReportVerticle.class.getSimpleName());
    }

    @Override
    protected String getAmqpContainerName() {
        return "dave/marginloader-PositionReportVerticle";
    }

    @Override
    protected String getAmqpQueueName() {
        return config().getJsonObject("listeners", new JsonObject()).getString("positionReport");
    }

    @Override
    protected void processObjectList(ObjectList.GPBObjectList gpbObjectList) {
        PrismaReports.PrismaHeader header = gpbObjectList.getHeader().getExtension(PrismaReports.prismaHeader);
        gpbObjectList.getItemList().forEach(gpbObject -> {
            if (gpbObject.hasExtension(PrismaReports.positionReport)) {
                PrismaReports.PositionReport positionReportData = gpbObject.getExtension(PrismaReports.positionReport);
                try {
                    PositionReportModel positionReportModel = new PositionReportModel(header, positionReportData);
                    this.persistenceService.store(positionReportModel, ModelType.POSITION_REPORT_MODEL, ar -> {
                        if (ar.succeeded()) {
                            LOG.debug("Position Report message processed");
                        } else {
                            LOG.error("Unable to store message", ar.cause());
                        }
                    });
                } catch (IllegalArgumentException ex) {
                    LOG.error("Unable to create Position Report Model from GPB data", ex);
                }
            } else {
                LOG.error("Unknown extension (should be {})", PrismaReports.positionReport.getDescriptor().getName());
            }
        });
    }

}