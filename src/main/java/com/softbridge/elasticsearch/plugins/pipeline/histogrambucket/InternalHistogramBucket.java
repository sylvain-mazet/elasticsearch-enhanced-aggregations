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
import org.elasticsearch.search.aggregations.Aggregations;
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

import static java.util.Collections.emptyList;

/**
 * Created by smazet on 07/02/18.
 */
public class InternalHistogramBucket extends InternalMultiBucketAggregation<InternalHistogramBucket,InternalHistogramBucket.Bucket>
    implements HistogramBucket {

    public static final String NAME = "histogram_bucket_value";

    InternalHistogramBucket(String name, List<Bucket> buckets, double interval, long minDocCount,
                            DocValueFormat formatter, List<PipelineAggregator> pipelineAggregators, Map<String, Object> metaData) {
        super(name,pipelineAggregators,metaData);
        this.buckets = buckets;
        this.interval = interval;
        this.minDocCount = minDocCount;
        this.format = formatter;
    }

    public InternalHistogramBucket(StreamInput in) throws IOException {
        super(in);
        interval = in.readDouble();
        minDocCount = in.readVLong();
        format = in.readNamedWriteable(DocValueFormat.class);
        buckets = in.readList(stream -> new InternalHistogramBucket.Bucket(stream, format));
    }

    private final List<Bucket> buckets;
    private final double interval;
    private final long minDocCount;
    private final DocValueFormat format;

    @Override
    public InternalHistogramBucket create(List<Bucket> buckets) {
        return new InternalHistogramBucket(name,buckets,interval,minDocCount,format,pipelineAggregators(),metaData);
    }

    @Override
    public Bucket createBucket(InternalAggregations aggregations, Bucket prototype) {
        return new Bucket(prototype.key,prototype.docCount, prototype.format, aggregations);
    }

    @Override
    public List<? extends InternalBucket> getBuckets() {
        return Collections.unmodifiableList(buckets);
    }

    @Override
    protected int doHashCode() {
        return Objects.hash(buckets, interval, format, minDocCount);
    }

    @Override
    protected boolean doEquals(Object obj) {
        InternalHistogramBucket that = (InternalHistogramBucket) obj;
        return Objects.equals(buckets, that.buckets)
                && Objects.equals(format, that.format)
                && Objects.equals(interval, that.interval)
                && Objects.equals(minDocCount, that.minDocCount);
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
        return new InternalHistogramBucket(getName(), emptyList(),interval,minDocCount,format,
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

    public static class Bucket extends InternalMultiBucketAggregation.InternalBucket implements Histogram.Bucket {
        double key;
        long docCount;
        private InternalAggregations aggregations;
        final transient DocValueFormat format;

        public Bucket(double key, long docCount, DocValueFormat format, InternalAggregations aggregations) {
            this.key = key;
            this.aggregations = aggregations;
            this.docCount = docCount;
            this.format = format;
        }

        public Bucket(StreamInput in, DocValueFormat format) throws IOException {
            this.key = in.readDouble();
            this.docCount = in.readLong();
            this.format = format;
            aggregations = InternalAggregations.readAggregations(in);
        }

        @Override
        public Object getKey() {
            return key;
        }

        @Override
        public String getKeyAsString() { return format.format(key); }

        @Override
        public long getDocCount() {
            return docCount;
        }

        @Override
        public Aggregations getAggregations() { return aggregations; }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeDouble(key);
            out.writeLong(docCount);
            aggregations.writeTo(out);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(CommonFields.KEY.getPreferredName(), key);
            builder.field(CommonFields.DOC_COUNT.getPreferredName(), docCount);
            if (null != format && format != DocValueFormat.RAW) {
                String keyAsString = format.format(key);
                builder.field(CommonFields.KEY_AS_STRING.getPreferredName(), keyAsString);
            }
            aggregations.toXContentInternal(builder, params);
            builder.endObject();
            return builder;
        }
    }

}
