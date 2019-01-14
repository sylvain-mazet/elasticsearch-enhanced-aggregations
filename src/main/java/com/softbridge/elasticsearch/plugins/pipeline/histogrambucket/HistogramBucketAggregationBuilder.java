/*
 * Copyright 2018 sOftbridge Technology
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.softbridge.elasticsearch.plugins.pipeline.histogrambucket;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.ParsingException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.BUCKETS_PATH;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.FORMAT;

/**
 * Created by smazet on 05/02/18.
 */
public class HistogramBucketAggregationBuilder extends AbstractPipelineAggregationBuilder<HistogramBucketAggregationBuilder> {

    public static final String NAME = "histogram_bucket";

    private final Map<String, String> bucketsPathsMap;
    private double interval;
    private double offset;
    private double minBound;
    private double maxBound;
    private long minDocCount;
    private String format;
    private InternalOrder order;

    private static final double DEFAULT_INTERVAL = 100;
    private static final double DEFAULT_OFFSET = 0;
    private static final double DEFAULT_MIN_BOUND = Double.MAX_VALUE;
    private static final double DEFAULT_MAX_BOUND = Double.MIN_VALUE;
    private static final long   DEFAULT_MIN_DOC_COUNT = 0;
    private static final InternalOrder DEFAULT_ORDER =  (InternalOrder) InternalOrder.KEY_ASC;

    public HistogramBucketAggregationBuilder(String name, double interval, double offset, double minBound, double maxBound,
                                             long minDocCount, String format, Map<String, String> bucketsPathsMap) {
        super(name, NAME, new TreeMap<>(bucketsPathsMap).values().toArray(new String[bucketsPathsMap.size()]));
        this.bucketsPathsMap = bucketsPathsMap;
        this.interval = interval;
        this.offset = offset;
        this.minBound = minBound;
        this.maxBound = maxBound;
        this.minDocCount = minDocCount;
        this.format = format;
    }

    public HistogramBucketAggregationBuilder(String name, double interval, double offset, double minBound, double maxBound,
                                             long minDocCount, String format, String... bucketsPaths) {
        this(name,interval,offset,minBound,maxBound,minDocCount,format,convertToBucketsPathMap(bucketsPaths));
    }

    public HistogramBucketAggregationBuilder(String name, Map<String, String> bucketsPathsMap) {
        super(name, NAME, new TreeMap<>(bucketsPathsMap).values().toArray(new String[bucketsPathsMap.size()]));
        this.bucketsPathsMap = bucketsPathsMap;
        this.interval = DEFAULT_INTERVAL;
        this.offset = DEFAULT_OFFSET;
        this.minBound = DEFAULT_MIN_BOUND;
        this.maxBound = DEFAULT_MAX_BOUND;
        this.minDocCount = DEFAULT_MIN_DOC_COUNT;
        this.format = null;
        this.order = DEFAULT_ORDER;
    }
    public HistogramBucketAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        this.interval = in.readDouble();
        this.offset = in.readDouble();
        this.minBound = in.readDouble();
        this.maxBound = in.readDouble();
        this.minDocCount = in.readVLong();
        this.format = in.readOptionalString();
        this.order = InternalOrder.Streams.readOrder(in);
        int mapSize = in.readVInt();
        bucketsPathsMap = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            bucketsPathsMap.put(in.readString(), in.readString());
        }
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(this.interval);
        out.writeDouble(this.offset);
        out.writeDouble(this.minBound);
        out.writeDouble(this.maxBound);
        out.writeVLong(this.minDocCount);
        out.writeOptionalString(format);
        InternalOrder.Streams.writeOrder(this.order,out);
        out.writeVInt(bucketsPathsMap.size());
        for (Map.Entry<String, String> e : bucketsPathsMap.entrySet()) {
            out.writeString(e.getKey());
            out.writeString(e.getValue());
        }
    }

    protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {
        return new HistogramBucketAggregator(name, interval, offset, minBound, maxBound,
                minDocCount, order, formatter(), bucketsPathsMap, metaData);
    }

    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(BUCKETS_PATH.getPreferredName(), bucketsPathsMap);
        builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), interval);
        builder.field(Histogram.MIN_DOC_COUNT_FIELD.getPreferredName(), minDocCount);
        if (format != null) {
            builder.field(FORMAT.getPreferredName(), format);
        }
        return builder;
    }

    private static Map<String, String> convertToBucketsPathMap(String[] bucketsPaths) {
        Map<String, String> bucketsPathsMap = new HashMap<>();
        for (int i = 0; i < bucketsPaths.length; i++) {
            bucketsPathsMap.put("_value" + i, bucketsPaths[i]);
        }
        return bucketsPathsMap;
    }

    private static final ObjectParser<double[], Void> EXTENDED_BOUNDS_PARSER = new ObjectParser<>(
            Histogram.EXTENDED_BOUNDS_FIELD.getPreferredName(),
            () -> new double[]{ Double.POSITIVE_INFINITY, Double.NEGATIVE_INFINITY });
    static {
        EXTENDED_BOUNDS_PARSER.declareDouble((bounds, d) -> bounds[0] = d, new ParseField("min"));
        EXTENDED_BOUNDS_PARSER.declareDouble((bounds, d) -> bounds[1] = d, new ParseField("max"));
    }

    public static HistogramBucketAggregationBuilder parse(String reducerName, XContentParser parser) throws IOException {
        XContentParser.Token token;
        String currentFieldName = null;
        Map<String, String> bucketsPathsMap = null;
        double interval = DEFAULT_INTERVAL;
        long minDocCount = DEFAULT_MIN_DOC_COUNT;
        double offset = DEFAULT_OFFSET;
        double minBound = DEFAULT_MIN_BOUND;
        double maxBound = DEFAULT_MAX_BOUND;
        InternalOrder order = DEFAULT_ORDER;
        String format = null;

        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();

                if (Histogram.EXTENDED_BOUNDS_FIELD.match(currentFieldName)) {
                    double bounds[] = EXTENDED_BOUNDS_PARSER.apply(parser,null);
                    minBound = bounds[0];
                    maxBound = bounds[1];
                } else if (Histogram.ORDER_FIELD.match(currentFieldName)) {
                    order = parseOrder(parser);
                }

            } else if (token == XContentParser.Token.VALUE_STRING) {
                if (BUCKETS_PATH.match(currentFieldName)) {
                    bucketsPathsMap = new HashMap<>();
                    bucketsPathsMap.put("_value", parser.text());
                } else if (FORMAT.match(currentFieldName)) {
                    format = parser.text();
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.VALUE_NUMBER) {
                 if (Histogram.INTERVAL_FIELD.match(currentFieldName)) {
                    interval = parser.doubleValue();
                } else if (Histogram.MIN_DOC_COUNT_FIELD.match(currentFieldName)) {
                    minDocCount = parser.longValue();
                } else if (Histogram.OFFSET_FIELD.match(currentFieldName)) {
                     offset = parser.longValue();
                 } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_ARRAY) {
                if (BUCKETS_PATH.match(currentFieldName)) {
                    List<String> paths = new ArrayList<>();
                    while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                        String path = parser.text();
                        paths.add(path);
                    }
                    bucketsPathsMap = new HashMap<>();
                    for (int i = 0; i < paths.size(); i++) {
                        bucketsPathsMap.put("_value" + i, paths.get(i));
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else if (token == XContentParser.Token.START_OBJECT) {
                if (BUCKETS_PATH.match(currentFieldName)) {
                    Map<String, Object> map = parser.map();
                    bucketsPathsMap = new HashMap<>();
                    for (Map.Entry<String, Object> entry : map.entrySet()) {
                        bucketsPathsMap.put(entry.getKey(), String.valueOf(entry.getValue()));
                    }
                } else {
                    throw new ParsingException(parser.getTokenLocation(),
                            "Unknown key for a " + token + " in [" + reducerName + "]: [" + currentFieldName + "].");
                }
            } else {
                throw new ParsingException(parser.getTokenLocation(), "Unexpected token " + token + " in [" + reducerName + "].");
            }
        }

        if (bucketsPathsMap == null) {
            throw new ParsingException(parser.getTokenLocation(), "Missing required field [" + BUCKETS_PATH.getPreferredName()
                    + "] for series_arithmetic aggregation [" + reducerName + "]");
        }

        HistogramBucketAggregationBuilder factory = new HistogramBucketAggregationBuilder(reducerName,interval,offset,
                minBound, maxBound, minDocCount, format,bucketsPathsMap);
        factory.order(order);
        return factory;
    }

    @Override
    protected int doHashCode() { return Objects.hash(bucketsPathsMap, interval, offset, minBound, maxBound, minDocCount, format, order); }

    @Override
    protected boolean doEquals(Object obj) {
        HistogramBucketAggregationBuilder other = (HistogramBucketAggregationBuilder) obj;
        return Objects.equals(bucketsPathsMap, other.bucketsPathsMap)&&Objects.equals(interval,other.interval)
                &&Objects.equals(offset,other.offset)&&Objects.equals(minBound,other.minBound)
                &&Objects.equals(maxBound,other.maxBound)&&Objects.equals(minDocCount,other.minDocCount)
                &&Objects.equals(format,other.format)&&Objects.equals(order.id(),other.order.id());
    }

    public String getWriteableName() { return NAME; }

    protected DocValueFormat formatter() {
        if (format != null) {
            return new DocValueFormat.Decimal(format);
        } else {
            return org.elasticsearch.search.DocValueFormat.RAW;
        }
    }

    private static InternalOrder parseOrder(XContentParser parser) throws IOException {
        InternalOrder order = null;
        XContentParser.Token token;
        String currentFieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token == XContentParser.Token.VALUE_STRING) {
                String dir = parser.text();
                boolean asc = "asc".equals(dir);
                if (!asc && !"desc".equals(dir)) {
                    throw new ParsingException(parser.getTokenLocation(), "Unknown order direction: [" + dir
                            + "]. Should be either [asc] or [desc]");
                }
                order = resolveOrder(currentFieldName, asc);
            }
        }
        return order;
    }

    private static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_key".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.KEY_ASC : InternalOrder.KEY_DESC);
        }
        if ("_count".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.COUNT_ASC : InternalOrder.COUNT_DESC);
        }
        throw new AggregationInitializationException("unimplemented key for order: "+key+ ", key must be _count or _key");
    }

    public HistogramBucketAggregationBuilder order(InternalHistogramBucket.Order order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        this.order = (InternalOrder) order;
        return this;
    }

    public double interval() {
        return interval;
    }

    public HistogramBucketAggregationBuilder interval(double interval) {
        if (interval <= 0) {
            throw new IllegalArgumentException("[interval] must be >0 for histogram aggregation [" + name + "]");
        }
        this.interval = interval;
        return this;
    }

    public double offset() { return offset; }

    public HistogramBucketAggregationBuilder offset(double offset) {
        this.offset = offset;
        return this;
    }

    public double minBound() {
        return minBound;
    }

    public double maxBound() {
        return maxBound;
    }

    public HistogramBucketAggregationBuilder extendedBounds(double minBound, double maxBound) {
        if (Double.isFinite(minBound) == false) {
            throw new IllegalArgumentException("minBound must be finite, got: " + minBound);
        }
        if (Double.isFinite(maxBound) == false) {
            throw new IllegalArgumentException("maxBound must be finite, got: " + maxBound);
        }
        if (maxBound < minBound) {
            throw new IllegalArgumentException("maxBound [" + maxBound + "] must be greater than minBound [" + minBound + "]");
        }
        this.minBound = minBound;
        this.maxBound = maxBound;
        return this;
    }

    public long minDocCount() {
        return minDocCount;
    }

    public HistogramBucketAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                    "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]");
        }
        this.minDocCount = minDocCount;
        return this;
    }

    public String format() {
        return format;
    }

    public HistogramBucketAggregationBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return this;
    }

}
