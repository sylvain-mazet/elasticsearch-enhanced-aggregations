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

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;
import org.elasticsearch.search.aggregations.pipeline.PipelineAggregator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Created by smazet on 22/02/18.
 */
public class InternalDateHistogramBucket
        extends InternalMultiBucketAggregation<InternalDateHistogramBucket, InternalHistogramBucket.Bucket>
        implements DateHistogramBucket {

    public static final String NAME = "date_histogram_bucket_value";

    InternalDateHistogramBucket(String name, List<InternalHistogramBucket.Bucket> buckets, double interval, long minDocCount,
                                DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name,pipelineAggregators,metaData);
        this.buckets = buckets;
        this.interval = interval;
        this.minDocCount = minDocCount;
        this.format = formatter;
    }

    public InternalDateHistogramBucket(StreamInput in) throws IOException {
        super(in);
        interval = in.readDouble();
        minDocCount = in.readVLong();
        format = in.readNamedWriteable(DocValueFormat.class);
        buckets = in.readList(stream -> new InternalHistogramBucket.Bucket(stream, format));
    }

    private final List<InternalHistogramBucket.Bucket> buckets;
    private final double interval;
    private final long minDocCount;
    private final DocValueFormat format;

    @Override
    protected int doHashCode() {
        return Objects.hash(buckets, interval, format, minDocCount);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalDateHistogramBucket that = (InternalDateHistogramBucket) obj;
        return Objects.equals(buckets, that.buckets)
                && Objects.equals(format, that.format)
                && Objects.equals(interval, that.interval)
                && Objects.equals(minDocCount, that.minDocCount);
    }

    @Override
    public InternalDateHistogramBucket create(List<InternalHistogramBucket.Bucket> buckets) {
        return new InternalDateHistogramBucket(name,buckets,interval,minDocCount,format,pipelineAggregators(),metaData);
    }

    @Override
    public InternalHistogramBucket.Bucket createBucket(InternalAggregations aggregations, InternalHistogramBucket.Bucket prototype) {
        return new InternalHistogramBucket.Bucket(prototype.key,prototype.docCount, prototype.format, aggregations);
    }

    @Override
    public List<? extends InternalBucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(interval);
        out.writeVLong(minDocCount);
        out.writeNamedWriteable(format);
        out.writeList(buckets);
    }

    @Override
    public InternalAggregation doReduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
        return new InternalDateHistogramBucket(getName(),Collections.emptyList(),interval,minDocCount,format,
                pipelineAggregators(),getMetaData());
    }

    @Override
    public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
        builder.field(Histogram.INTERVAL_FIELD.getPreferredName(),interval);
        builder.field(Histogram.MIN_DOC_COUNT_FIELD.getPreferredName(),minDocCount);
        builder.startArray(CommonFields.BUCKETS.getPreferredName());
        for (InternalHistogramBucket.Bucket bucket : buckets) {
            bucket.toXContent(builder, params);
        }
        builder.endArray();
        return builder;
    }

    @Override
    public String getWriteableName() {
        return NAME;
    }
}
