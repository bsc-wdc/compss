/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.wdc.tracing.monitor;

import com.sun.management.OperatingSystemMXBean;
import es.bsc.wdc.tracing.Loggers;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.ObservableDoubleMeasurement;
import io.opentelemetry.api.metrics.ObservableLongMeasurement;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.SdkMeterProviderBuilder;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.ResourceAttributes;

import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.util.function.Consumer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class OtelMetrics {

    // Constants
    private static final String METER_NAME = "es.bsc.compss.monitor";
    private static final String ATTR_NODENAME = "compss.node.name";
    private static final String ATTR_MASTERNAME = "compss.master.name";

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);

    private final Meter meter;


    /**
     * Constructs a new OTelMetrics exporting CPU and memory metrics.
     *
     * @param otlpEndpoint endpoint where to submit Otel metrics
     * @param serviceName name of the service
     * @param nodeName name of the local node
     * @param masterName name of the master node
     */
    public OtelMetrics(String otlpEndpoint, String serviceName, String nodeName, String masterName) {
        if (masterName == null || masterName.trim().isEmpty()) {
            masterName = Constants.DEFAULT_MASTER_NAME;
        }
        if (serviceName == null || serviceName.isEmpty()) {
            serviceName = Constants.DEFAULT_SERVICE_NAME;
        }

        // OTel Resource (required for tagging metrics)
        Resource res = constructOtelResource(nodeName, masterName, serviceName);

        if (otlpEndpoint == null || otlpEndpoint.isEmpty()) {
            otlpEndpoint = Constants.DEFAULT_OTEL_ENDPOINT;
        }
        Duration period = Constants.DEFAULT_OTEL_PERIOD;
        this.meter = generatePeriodicMeter(otlpEndpoint, res, period);

        // CPU/Mem
        final OperatingSystemMXBean osBean = getOSBean();
        if (osBean == null) {
            addDoubleGauge("cpu.percent", "Host CPU percentage", "%", m -> {
            });
            addLongGauge("mem.bytes.used", "Host used memory", "bytes", m -> {
            });
            addLongGauge("mem.bytes.total", "Host total memory", "bytes", m -> {
            });
            addLongGauge("mem.percentage_used", "Host memory utilization percentage", "%", m -> {
            });
        } else {
            final Attributes commonAttrs = constructCommonAttributes(nodeName, masterName);

            addDoubleGauge("cpu.percent", "Host CPU percentage", "%", m -> {
                double load = osBean.getSystemCpuLoad();
                m.record(load * 100.0, commonAttrs);
            });

            addLongGauge("mem.bytes.used", "Host used memory", "bytes", m -> {
                long tot = osBean.getTotalPhysicalMemorySize();
                long free = osBean.getFreePhysicalMemorySize();
                m.record(Math.max(0, tot - free), commonAttrs);
            });

            addLongGauge("mem.bytes.total", "Host total memory", "bytes", m -> {
                long tot = osBean.getTotalPhysicalMemorySize();
                m.record(tot, commonAttrs);
            });

            addLongGauge("mem.percentage_used", "Host memory utilization percentage", "%", m -> {
                long tot = osBean.getTotalPhysicalMemorySize();
                long free = osBean.getFreePhysicalMemorySize();
                long used = Math.max(0, tot - free);
                long perc = (tot > 0) ? (used * 100L) / tot : 0L;
                m.record(perc, commonAttrs);
            });
        }

    }

    private Attributes constructCommonAttributes(String nodeName, String masterName) {
        AttributesBuilder ab = Attributes.builder();
        ab = ab.put(ATTR_MASTERNAME, masterName);
        ab = ab.put(ATTR_NODENAME, nodeName);
        return ab.build();
    }

    private Resource constructOtelResource(String nodeName, String masterName, String serviceName) {
        AttributesBuilder ab = Attributes.builder();
        ab = ab.put(ResourceAttributes.SERVICE_NAME, serviceName);
        ab = ab.put(ATTR_MASTERNAME, masterName);
        ab = ab.put(ATTR_NODENAME, nodeName);
        Attributes attrs = ab.build();

        Resource res = Resource.create(attrs);
        return Resource.getDefault().merge(res);
    }

    private static Meter generatePeriodicMeter(String otlpEndpoint, Resource res, Duration period) {

        OtlpGrpcMetricExporter metricExporter = OtlpGrpcMetricExporter.builder().setEndpoint(otlpEndpoint).build();
        PeriodicMetricReader metricReader = PeriodicMetricReader.builder(metricExporter).setInterval(period).build();

        SdkMeterProviderBuilder mpBuilder = SdkMeterProvider.builder();
        mpBuilder = mpBuilder.setResource(res);
        mpBuilder = mpBuilder.registerMetricReader(metricReader);
        SdkMeterProvider mp = mpBuilder.build();

        OpenTelemetrySdk otel = OpenTelemetrySdk.builder().setMeterProvider(mp).buildAndRegisterGlobal();
        return otel.getMeter(METER_NAME);
    }

    private static OperatingSystemMXBean getOSBean() {
        try {
            return (OperatingSystemMXBean) ManagementFactory.getOperatingSystemMXBean();
        } catch (Throwable t) {
            LOGGER.warn("Could not retrieve OSBean. Information will CPU and Memory information will not be available");
            return null;
        }
    }

    /**
     * Registers a new gauge for a long metric.
     *
     * @param name name of the metric
     * @param desc description of the metric
     * @param unit unit of the metric
     * @param callback callback for metric measurement
     */
    public void addLongGauge(String name, String desc, String unit, Consumer<ObservableLongMeasurement> callback) {
        meter.gaugeBuilder(name).ofLongs().setDescription(desc).setUnit(unit).buildWithCallback(callback);
    }

    /**
     * Registers a new gauge for a double metric.
     *
     * @param name name of the metric
     * @param desc description of the metric
     * @param unit unit of the metric
     * @param callback callback for metric measurement
     */
    public void addDoubleGauge(String name, String desc, String unit, Consumer<ObservableDoubleMeasurement> callback) {
        meter.gaugeBuilder(name).setDescription(desc).setUnit(unit).buildWithCallback(callback);
    }
}
