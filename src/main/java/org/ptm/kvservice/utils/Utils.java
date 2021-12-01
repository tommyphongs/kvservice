package org.ptm.kvservice.utils;

import io.prometheus.client.Collector;

import java.io.IOException;
import java.util.Enumeration;
import java.util.Iterator;

public class Utils {

    private static final char ESCAPES[] = { '$', '^', '[', ']', '(', ')', '{', '|', '+', '\\', '.', '<', '>' };

    public static String wildcaldToRegexp(String pattern) {
        String result = "^";
        for (int i = 0; i < pattern.length(); i++) {
            char ch = pattern.charAt(i);
            boolean escaped = false;
            for (int j = 0; j < ESCAPES.length; j++) {
                if (ch == ESCAPES[j]) {
                    result += "\\" + ch;
                    escaped = true;
                    break;
                }
            }

            if (!escaped) {
                if (ch == '*') {
                    result += ".*";
                } else if (ch == '?') {
                    result += ".";
                } else {
                    result += ch;
                }
            }
        }
        result += "$";
        return result;
    }

    private static void writeEscapedLabelValue(StringBuilder writer, String s) throws IOException {
        for(int i = 0; i < s.length(); ++i) {
            char c = s.charAt(i);
            switch(c) {
                case '\n':
                    writer.append("\\n");
                    break;
                case '"':
                    writer.append("\\\"");
                    break;
                case '\\':
                    writer.append("\\\\");
                    break;
                default:
                    writer.append(c);
            }
        }

    }

    public static String writeOpenMetrics100(Enumeration<Collector.MetricFamilySamples> mfs) throws IOException {
        StringBuilder stringBuilder = new StringBuilder();
        while(mfs.hasMoreElements()) {
            Collector.MetricFamilySamples metricFamilySamples = (Collector.MetricFamilySamples)mfs.nextElement();
            String name = metricFamilySamples.name;
            stringBuilder.append("# TYPE ");
            stringBuilder.append(name);
            stringBuilder.append(" ");
            stringBuilder.append(omTypeString(metricFamilySamples.type));
            stringBuilder.append("\n");
            if (!metricFamilySamples.unit.isEmpty()) {
                stringBuilder.append("# UNIT ");
                stringBuilder.append(name);
                stringBuilder.append(" ");
                stringBuilder.append(metricFamilySamples.unit);
                stringBuilder.append("\n");
            }

            stringBuilder.append("# HELP ");
            stringBuilder.append(name);
            stringBuilder.append(" ");
            writeEscapedLabelValue(stringBuilder, metricFamilySamples.help);
            stringBuilder.append("\n");

            for(Iterator var4 = metricFamilySamples.samples.iterator(); var4.hasNext(); stringBuilder.append("\n")) {
                Collector.MetricFamilySamples.Sample sample = (Collector.MetricFamilySamples.Sample)var4.next();
                stringBuilder.append(sample.name);
                if (sample.labelNames.size() > 0) {
                    stringBuilder.append("{");

                    for(int i = 0; i < sample.labelNames.size(); ++i) {
                        if (i > 0) {
                            stringBuilder.append(",");
                        }

                        stringBuilder.append((String)sample.labelNames.get(i));
                        stringBuilder.append("=\"");
                        writeEscapedLabelValue(stringBuilder, (String)sample.labelValues.get(i));
                        stringBuilder.append("\"");
                    }

                    stringBuilder.append("}");
                }

                stringBuilder.append(" ");
                stringBuilder.append(Collector.doubleToGoString(sample.value));
                if (sample.timestampMs != null) {
                    stringBuilder.append(" ");
                    long ts = sample.timestampMs;
                    stringBuilder.append(Long.toString(ts / 1000L));
                    stringBuilder.append(".");
                    long ms = ts % 1000L;
                    if (ms < 100L) {
                        stringBuilder.append("0");
                    }

                    if (ms < 10L) {
                        stringBuilder.append("0");
                    }

                    stringBuilder.append(Long.toString(ts % 1000L));
                }
            }
        }

        stringBuilder.append("# EOF\n");
        return stringBuilder.toString();
    }

    private static String omTypeString(Collector.Type t) {
        switch(t) {
            case GAUGE:
                return "gauge";
            case COUNTER:
                return "counter";
            case SUMMARY:
                return "summary";
            case HISTOGRAM:
                return "histogram";
            case GAUGE_HISTOGRAM:
                return "gauge_histogram";
            case STATE_SET:
                return "stateset";
            case INFO:
                return "info";
            default:
                return "unknown";
        }
    }

}
