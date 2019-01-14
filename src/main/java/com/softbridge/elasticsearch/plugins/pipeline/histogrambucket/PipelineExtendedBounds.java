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

import org.elasticsearch.common.CheckedFunction;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;

import java.io.IOException;
import java.util.Objects;
import java.util.function.LongSupplier;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.optionalConstructorArg;

/**
 * Created by smazet on 15/02/18.
 *
 * Copied from org.elasticsearch.search.aggregations.bucket.histogram.ExtendedBounds.
 * I just need to parseAndValidate outside of a search context.
 */
public class PipelineExtendedBounds implements ToXContent, Writeable {
    private static final ParseField EXTENDED_BOUNDS_FIELD = Histogram.EXTENDED_BOUNDS_FIELD;
    private static final ParseField MIN_FIELD = new ParseField("min");
    private static final ParseField MAX_FIELD = new ParseField("max");

    static final ConstructingObjectParser<PipelineExtendedBounds, Void> PARSER = new ConstructingObjectParser<>(
            "extended_bounds", a -> {
        assert a.length == 2;
        Long min = null;
        Long max = null;
        String minAsStr = null;
        String maxAsStr = null;
        if (a[0] == null) {
            // nothing to do with it
        } else if (a[0] instanceof Long) {
            min = (Long) a[0];
        } else if (a[0] instanceof String) {
            minAsStr = (String) a[0];
        } else {
            throw new IllegalArgumentException("Unknown field type [" + a[0].getClass() + "]");
        }
        if (a[1] == null) {
            // nothing to do with it
        } else if (a[1] instanceof Long) {
            max = (Long) a[1];
        } else if (a[1] instanceof String) {
            maxAsStr = (String) a[1];
        } else {
            throw new IllegalArgumentException("Unknown field type [" + a[1].getClass() + "]");
        }
        return new PipelineExtendedBounds(min, max, minAsStr, maxAsStr);
    });
    static {
        CheckedFunction<XContentParser, Object, IOException> longOrString = p -> {
            if (p.currentToken() == XContentParser.Token.VALUE_NUMBER) {
                return p.longValue(false);
            }
            if (p.currentToken() == XContentParser.Token.VALUE_STRING) {
                return p.text();
            }
            if (p.currentToken() == XContentParser.Token.VALUE_NULL) {
                return null;
            }
            throw new IllegalArgumentException("Unsupported token [" + p.currentToken() + "]");
        };
        PARSER.declareField(optionalConstructorArg(), longOrString, MIN_FIELD, ObjectParser.ValueType.LONG_OR_NULL);
        PARSER.declareField(optionalConstructorArg(), longOrString, MAX_FIELD, ObjectParser.ValueType.LONG_OR_NULL);
    }

    /**
     * Parsed min value. If this is null and {@linkplain #minAsStr} isn't then this must be parsed from {@linkplain #minAsStr}. If this is
     * null and {@linkplain #minAsStr} is also null then there is no lower bound.
     */
    private final Long min;
    /**
     * Parsed min value. If this is null and {@linkplain #maxAsStr} isn't then this must be parsed from {@linkplain #maxAsStr}. If this is
     * null and {@linkplain #maxAsStr} is also null then there is no lower bound.
     */
    private final Long max;

    private final String minAsStr;
    private final String maxAsStr;

    /**
     * Construct with parsed bounds.
     */
    PipelineExtendedBounds(Long min, Long max) {
        this(min, max, null, null);
    }

    /**
     * Construct with unparsed bounds.
     *
     * @param minAsStr minimum value
     * @param maxAsStr maximum value
     */
    public PipelineExtendedBounds(String minAsStr, String maxAsStr) {
        this(null, null, minAsStr, maxAsStr);
    }

    private PipelineExtendedBounds(Long min, Long max, String minAsStr, String maxAsStr) {
        this.min = min;
        this.max = max;
        this.minAsStr = minAsStr;
        this.maxAsStr = maxAsStr;
    }

    /**
     * Read from a stream.
     */
    PipelineExtendedBounds(StreamInput in) throws IOException {
        min = in.readOptionalLong();
        max = in.readOptionalLong();
        minAsStr = in.readOptionalString();
        maxAsStr = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeOptionalLong(min);
        out.writeOptionalLong(max);
        out.writeOptionalString(minAsStr);
        out.writeOptionalString(maxAsStr);
    }

    /**
     * Parse the bounds and perform any delayed validation. Returns the result of the parsing.
     */
    PipelineExtendedBounds parseAndValidate(String aggName, DocValueFormat format) {
        Long min = this.min;
        Long max = this.max;
        assert format != null;
        if (minAsStr != null) {
            min = format.parseLong(minAsStr, false, new LongSupplier() {
                @Override
                public long getAsLong() {
                    return System.currentTimeMillis();
                }
            });
        }
        if (maxAsStr != null) {
            // TODO: Should we rather pass roundUp=true?
            max = format.parseLong(maxAsStr, false, new LongSupplier() {
                @Override
                public long getAsLong() {
                    return System.currentTimeMillis();
                }
            });
        }

        return new PipelineExtendedBounds(min, max, minAsStr, maxAsStr);
    }

    PipelineExtendedBounds round(Rounding rounding) {
        return new PipelineExtendedBounds(min != null ? rounding.round(min) : null, max != null ? rounding.round(max) : null);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(EXTENDED_BOUNDS_FIELD.getPreferredName());
        if (min != null) {
            builder.field(MIN_FIELD.getPreferredName(), min);
        } else {
            builder.field(MIN_FIELD.getPreferredName(), minAsStr);
        }
        if (max != null) {
            builder.field(MAX_FIELD.getPreferredName(), max);
        } else {
            builder.field(MAX_FIELD.getPreferredName(), maxAsStr);
        }
        builder.endObject();
        return builder;
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, minAsStr, maxAsStr);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        PipelineExtendedBounds other = (PipelineExtendedBounds) obj;
        return Objects.equals(min, other.min)
                && Objects.equals(max, other.max)
                && Objects.equals(minAsStr, other.minAsStr)
                && Objects.equals(maxAsStr, other.maxAsStr);
    }

    Long getMin() {
        return min;
    }

    Long getMax() {
        return max;
    }

    @Override
    public String toString() {
        StringBuilder b = new StringBuilder();
        if (min != null) {
            b.append(min);
            if (minAsStr != null) {
                b.append('(').append(minAsStr).append(')');
            }
        } else {
            if (minAsStr != null) {
                b.append(minAsStr);
            }
        }
        b.append("--");
        if (max != null) {
            b.append(min);
            if (maxAsStr != null) {
                b.append('(').append(maxAsStr).append(')');
            }
        } else {
            if (maxAsStr != null) {
                b.append(maxAsStr);
            }
        }
        return b.toString();
    }
}
