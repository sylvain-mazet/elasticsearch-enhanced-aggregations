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
import org.elasticsearch.common.joda.FormatDateTimeFormatter;
import org.elasticsearch.common.joda.Joda;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.AggregationInitializationException;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.pipeline.AbstractPipelineAggregationBuilder;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.TreeMap;
import java.util.HashMap;
import java.util.Map;
import java.util.List;
import java.util.ArrayList;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.XContentParser.Token.VALUE_NUMBER;
import static org.elasticsearch.search.aggregations.pipeline.PipelineAggregator.Parser.FORMAT;


/**
 * Created by smazet on 12/02/18.
 */
public class DateHistogramBucketAggregationBuilder extends AbstractPipelineAggregationBuilder<DateHistogramBucketAggregationBuilder> {

    public static final String NAME = "date_histogram_bucket";
    private static final String defaultDateFormat = "YYYY-MM-dd HH:mm:ss";

    private Map<String, String> bucketsPathsMap;
    private long interval = 100 ;
    private DateHistogramInterval dateHistogramInterval = null;
    private long offset = 0;
    private String format = defaultDateFormat;
    private PipelineExtendedBounds extendedBounds;
    private InternalOrder order = (InternalOrder) InternalOrder.KEY_ASC;
    private long minDocCount = 0;

    public DateHistogramBucketAggregationBuilder(String name, Map<String, String> bucketsPathsMap) {
        super(name, NAME, new TreeMap<>(bucketsPathsMap).values().toArray(new String[bucketsPathsMap.size()]));
        this.bucketsPathsMap = bucketsPathsMap;
    }

    public DateHistogramBucketAggregationBuilder(StreamInput in) throws IOException {
        super(in, NAME);
        this.interval = in.readLong();
        this.dateHistogramInterval = in.readOptionalWriteable(DateHistogramInterval::new);
        this.offset = in.readLong();
        this.extendedBounds = in.readOptionalWriteable(PipelineExtendedBounds::new);
        this.minDocCount = in.readVLong();
        this.format = in.readOptionalString();
        this.order = InternalOrder.Streams.readOrder(in);
        int mapSize = in.readVInt();
        bucketsPathsMap = new HashMap<>(mapSize);
        for (int i = 0; i < mapSize; i++) {
            bucketsPathsMap.put(in.readString(), in.readString());
        }
    }

    private static final ObjectParser<DateHistogramBucketAggregationBuilder, Void> PARSER;
    static {
        PARSER = new ObjectParser<>(DateHistogramBucketAggregationBuilder.NAME);
        PARSER.declareField(DateHistogramBucketAggregationBuilder::format, XContentParser::text,
                new ParseField("format"), ObjectParser.ValueType.STRING);

        PARSER.declareField((histogram, interval) -> {
            if (interval instanceof Long) {
                histogram.interval((long) interval);
            } else {
                histogram.dateHistogramInterval((DateHistogramInterval) interval);
            }
        }, p -> {
            if (p.currentToken() == VALUE_NUMBER) {
                return p.longValue();
            } else {
                return new DateHistogramInterval(p.text());
            }
        }, Histogram.INTERVAL_FIELD, ObjectParser.ValueType.LONG);

        PARSER.declareField(DateHistogramBucketAggregationBuilder::offset, (XContentParser p) -> {
            if (p.currentToken() == VALUE_NUMBER) {
                return p.longValue();
            } else {
                return DateHistogramBucketAggregationBuilder.parseStringOffset(p.text());
            }
        }, Histogram.OFFSET_FIELD, ObjectParser.ValueType.LONG);

        PARSER.declareLong(DateHistogramBucketAggregationBuilder::minDocCount, Histogram.MIN_DOC_COUNT_FIELD);

        PARSER.declareField(DateHistogramBucketAggregationBuilder::extendedBounds,
                parser -> PipelineExtendedBounds.PARSER.apply(parser, null),
                Histogram.EXTENDED_BOUNDS_FIELD, ObjectParser.ValueType.OBJECT);

        PARSER.declareField(DateHistogramBucketAggregationBuilder::order, DateHistogramBucketAggregationBuilder::parseOrder,
                Histogram.ORDER_FIELD, ObjectParser.ValueType.OBJECT);

        PARSER.declareField(DateHistogramBucketAggregationBuilder::bucketsPathsMap, DateHistogramBucketAggregationBuilder::parseBucketPath,
                BUCKETS_PATH_FIELD, ObjectParser.ValueType.OBJECT_OR_STRING);
    }

    public static DateHistogramBucketAggregationBuilder parse(String aggregationName, XContentParser parser) throws IOException {
        return PARSER.parse(parser, new DateHistogramBucketAggregationBuilder(aggregationName, new TreeMap<>()), null);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeLong(interval);
        out.writeOptionalWriteable(dateHistogramInterval);
        out.writeLong(offset);
        out.writeOptionalWriteable(extendedBounds);
        out.writeVLong(minDocCount);
        out.writeOptionalString(format);
        InternalOrder.Streams.writeOrder(this.order,out);
        out.writeVInt(bucketsPathsMap.size());
        for (Map.Entry<String, String> e : bucketsPathsMap.entrySet()) {
            out.writeString(e.getKey());
            out.writeString(e.getValue());
        }

    }

    public DateHistogramBucketAggregationBuilder bucketsPathsMap(Map<String, String> bucketsPathsMap) {
        this.bucketsPathsMap = bucketsPathsMap;
        return this;
    }

    static Map<String, String> parseBucketPath(XContentParser parser) throws IOException {
        Map<String, String> bucketsPathsMap = new HashMap<>(1);
        XContentParser.Token token = parser.currentToken();
        if (token == XContentParser.Token.FIELD_NAME) {
            // useless field name
        } else if (token == XContentParser.Token.VALUE_STRING) {
            bucketsPathsMap.put("_value", parser.text());
        } else if (token == XContentParser.Token.START_OBJECT) {
            Map<String, Object> map = parser.map();
            bucketsPathsMap = new HashMap<>();
            for (Map.Entry<String, Object> entry : map.entrySet()) {
                bucketsPathsMap.put(entry.getKey(), String.valueOf(entry.getValue()));
            }
        } else if (token == XContentParser.Token.START_ARRAY) {
            List<String> paths = new ArrayList<>();
            while ((token = parser.nextToken()) != XContentParser.Token.END_ARRAY) {
                String path = parser.text();
                paths.add(path);
            }
            bucketsPathsMap = new HashMap<>();
            for (int i = 0; i < paths.size(); i++) {
                bucketsPathsMap.put("_value" + i, paths.get(i));
            }
        }
        return bucketsPathsMap;
    }

    public double interval() {
        return interval;
    }

    public DateHistogramBucketAggregationBuilder interval(long interval) {
        if (interval < 1) {
            throw new IllegalArgumentException("[interval] must be 1 or greater for date histogram aggregation [" + name + "]");
        }
        this.interval = interval;
        // invalidate the date histogram interval
        this.dateHistogramInterval = null;
        return this;
    }

    public DateHistogramInterval dateHistogramInterval() {
        return dateHistogramInterval;
    }

    public DateHistogramBucketAggregationBuilder dateHistogramInterval(DateHistogramInterval dateHistogramInterval) {
        if (dateHistogramInterval == null) {
            throw new IllegalArgumentException("[dateHistogramInterval] must not be null: [" + name + "]");
        }
        this.dateHistogramInterval = dateHistogramInterval;
        // interval field will be ocerwritten in the aggregator
        return this;
    }

    public long offset() {
        return offset;
    }

    public DateHistogramBucketAggregationBuilder offset(long offset) {
        this.offset = offset;
        return this;
    }

    public DateHistogramBucketAggregationBuilder offset(String offset) {
        if (offset == null) {
            throw new IllegalArgumentException("[offset] must not be null: [" + name + "]");
        }
        return offset(parseStringOffset(offset));
    }

    static long parseStringOffset(String offset) {
        if (offset.charAt(0) == '-') {
            return -TimeValue
                    .parseTimeValue(offset.substring(1), null, DateHistogramBucketAggregationBuilder.class.getSimpleName() + ".parseOffset")
                    .millis();
        }
        int beginIndex = offset.charAt(0) == '+' ? 1 : 0;
        return TimeValue
                .parseTimeValue(offset.substring(beginIndex), null,
                                DateHistogramBucketAggregationBuilder.class.getSimpleName() + ".parseOffset")
                .millis();
    }

    public String format() {return format;}

    public DateHistogramBucketAggregationBuilder format(String format) {
        if (format == null) {
            throw new IllegalArgumentException("[format] must not be null: [" + name + "]");
        }
        this.format = format;
        return this;
    }

    public long minDocCount() { return minDocCount; }

    public DateHistogramBucketAggregationBuilder minDocCount(long minDocCount) {
        if (minDocCount < 0) {
            throw new IllegalArgumentException(
                    "[minDocCount] must be greater than or equal to 0. Found [" + minDocCount + "] in [" + name + "]");
        }
        this.minDocCount = minDocCount;
        return this;
    }

    public PipelineExtendedBounds extendedBounds() { return extendedBounds; }

    public DateHistogramBucketAggregationBuilder extendedBounds(PipelineExtendedBounds extendedBounds) {
        if (extendedBounds == null) {
            throw new IllegalArgumentException("[extendedBounds] must not be null: [" + name + "]");
        }
        this.extendedBounds = extendedBounds;
        return this;
    }

    public InternalHistogramBucket.Order order() {
        return order;
    }

    /** Set a new order on this builder and return the builder so that calls
     *  can be chained.
     *
     *  @param order order descriptor
     *  @return BucketDateHistogramAggregationBuilder */
    public DateHistogramBucketAggregationBuilder order(InternalHistogramBucket.Order order) {
        if (order == null) {
            throw new IllegalArgumentException("[order] must not be null: [" + name + "]");
        }
        this.order = (InternalOrder) order;
        return this;
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

    static InternalOrder resolveOrder(String key, boolean asc) {
        if ("_key".equals(key) || "_time".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.KEY_ASC : InternalOrder.KEY_DESC);
        }
        if ("_count".equals(key)) {
            return (InternalOrder) (asc ? InternalOrder.COUNT_ASC : InternalOrder.COUNT_DESC);
        }
        throw new AggregationInitializationException("unimplemented key for order: "+key+ ", key must be _count, _time or _key");
    }


    @Override
    protected PipelineAggregator createInternal(Map<String, Object> metaData) throws IOException {

        // ceinture _et_ bretelles
        if (format == null) { format = defaultDateFormat; }

        FormatDateTimeFormatter dateTimeFormatter = Joda.forPattern(format);
        DocValueFormat formatter = new DocValueFormat.DateTime(dateTimeFormatter, DateTimeZone.UTC);

        PipelineExtendedBounds roundedBounds = null;
        if (null != extendedBounds) {
            roundedBounds = extendedBounds.parseAndValidate(name,formatter);
        } else {
            roundedBounds = new PipelineExtendedBounds(Long.MAX_VALUE,Long.MIN_VALUE);
            roundedBounds = roundedBounds.parseAndValidate(name,formatter);
        }

        return new DateHistogramBucketAggregator(name, dateHistogramInterval, interval, offset, minDocCount, formatter,
                roundedBounds, order, bucketsPathsMap, metaData);
    }

    @Override
    protected XContentBuilder internalXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(BUCKETS_PATH_FIELD.getPreferredName(), bucketsPathsMap);
        builder.field(Histogram.INTERVAL_FIELD.getPreferredName(), interval);
        builder.field(Histogram.MIN_DOC_COUNT_FIELD.getPreferredName(), minDocCount);
        if (format != null) {
            builder.field(FORMAT.getPreferredName(), format);
        }
        return builder;
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(bucketsPathsMap, dateHistogramInterval, interval, offset, extendedBounds, minDocCount, format, order);
    }

    @Override
    protected boolean doEquals(Object obj) {
        DateHistogramBucketAggregationBuilder other = (DateHistogramBucketAggregationBuilder) obj;
        return Objects.equals(bucketsPathsMap, other.bucketsPathsMap)
                &&Objects.equals(dateHistogramInterval,other.dateHistogramInterval)
                &&Objects.equals(interval,other.interval)
                &&Objects.equals(offset,other.offset)&&Objects.equals(extendedBounds,other.extendedBounds)
                &&Objects.equals(minDocCount,other.minDocCount)
                &&Objects.equals(format,other.format)&&Objects.equals(order.id(),other.order.id());
    }

    @Override
    public String getWriteableName() { return NAME; }
}
