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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.util.CollectionUtil;
import org.apache.lucene.util.PriorityQueue;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.rounding.DateTimeUnit;
import org.elasticsearch.common.rounding.Rounding;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramInterval;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import static org.elasticsearch.search.aggregations.bucket.histogram.DateHistogramAggregationBuilder.DATE_FIELD_UNITS;

/**
 * Created by smazet on 12/02/18.
 */
public class DateHistogramBucketAggregator extends SiblingPipelineAggregator {

    private Logger logger = LogManager.getLogger(DateHistogramBucketAggregator.class);

    private final Map<String, String> bucketsPathsMap;
    private final DateHistogramInterval dateHistogramInterval;
    private long interval;
    private final long offset;
    private final long minDocCount;
    private final DocValueFormat format;
    private final PipelineExtendedBounds extendedBounds;
    private final InternalOrder order;

    DateHistogramBucketAggregator(String name, DateHistogramInterval dateHistogramInterval, long interval, long offset, long minDocCount,
                                  DocValueFormat format, PipelineExtendedBounds extendedBounds, InternalOrder order,
                                  Map<String, String> bucketsPathsMap, Map<String, Object> metaData) {
        super(name,new TreeMap<>(bucketsPathsMap).values().toArray(new String[bucketsPathsMap.size()]),metaData);

        this.dateHistogramInterval = dateHistogramInterval;
        this.interval = interval;
        this.offset = offset;
        this.minDocCount = minDocCount;
        this.format = format;
        this.extendedBounds = extendedBounds;
        this.order = order;
        this.bucketsPathsMap = bucketsPathsMap;

    }

    @SuppressWarnings("unchecked")
    public DateHistogramBucketAggregator(StreamInput in) throws IOException {
        super(in);
        this.dateHistogramInterval = in.readOptionalWriteable(DateHistogramInterval::new);
        this.interval = in.readLong();
        this.offset = in.readLong();
        this.minDocCount = in.readVLong();
        this.format = in.readNamedWriteable(DocValueFormat.class);
        this.extendedBounds = in.readOptionalWriteable(PipelineExtendedBounds::new);
        this.order = InternalOrder.Streams.readOrder(in);
        this.bucketsPathsMap = (Map<String, String>)in.readGenericValue();
    }

    @Override
    public InternalAggregation doReduce(Aggregations aggregations, InternalAggregation.ReduceContext context) {

        Rounding rounding = createRounding();

        List<String> bucketsPath = AggregationPath.parse(bucketsPaths()[0]).getPathElementsAsStringList();
        LongHash bucketOrds = new LongHash(1,context.bigArrays());
        PriorityQueue<Long> pq = null;
        LongArray counts = null;

        logger.debug("  START date histogram bucket reduction");
        logger.debug("  INTERVAL: "+rounding.nextRoundingValue(0));
        logger.debug("  OFFSET: "+offset);
        logger.debug("  EXTENDED BOUNDS: "+extendedBounds.getMin()+" to "+extendedBounds.getMax());
        logger.debug("  EXTENDED BOUNDS: "+format.format(extendedBounds.getMin())+" to "+format.format(extendedBounds.getMax()));

        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(bucketsPath.get(0))) {
                bucketsPath = bucketsPath.subList(1, bucketsPath.size());
                InternalMultiBucketAggregation<?, ?> multiBucketsAgg = (InternalMultiBucketAggregation<?, ?>) aggregation;
                List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = multiBucketsAgg.getBuckets();

                counts = context.bigArrays().newLongArray(buckets.size());

                pq = new PriorityQueue<Long>(buckets.size()) {
                    @Override
                    protected boolean lessThan(Long a, Long b) {
                        return a < b;
                    }
                };

                for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                    Double bucketValue = BucketHelpers.resolveBucketValue(multiBucketsAgg, bucket, bucketsPath,
                            BucketHelpers.GapPolicy.SKIP);

                    if (bucketValue != null) {

                        Long value = Math.round(bucketValue);

                        long key = rounding.round(value - offset ) + offset;

                        long bucketOrd = bucketOrds.add(key);

                        if (bucketOrd < 0) { // already seen
                            // collectExistingBucket
                            bucketOrd = -1 - bucketOrd;
                            counts.increment(bucketOrd,1);
                        } else {
                            // collectBucket
                            pq.add(key);
                            counts.set(bucketOrd,1);
                        }

                    }
                }
            }
        }

        if (null==counts) {
            throw new AggregationExecutionException("Could not find aggregation path");
        }

        List<InternalHistogramBucket.Bucket> listOfBuckets = new ArrayList<>(4);
        long nextExpectedKey;
        if (extendedBounds.getMin().equals(Long.MAX_VALUE)) {
            nextExpectedKey = Long.MAX_VALUE;
        } else {
            nextExpectedKey = rounding.round(extendedBounds.getMin() - offset) + offset;
        }
        while(pq.size()>0) {
            long key = pq.pop();

            if (minDocCount == 0) {
                while (nextExpectedKey < key) {
                    listOfBuckets.add(new InternalHistogramBucket.Bucket(nextExpectedKey, 0,
                            format, new InternalAggregations(Collections.emptyList())));
                    // cannot overflow: if we are here, nextExpectedKey can not be MAX_VALUE
                    nextExpectedKey += interval;
                }
            }
            nextExpectedKey = key + interval;

            long bucketOrd = bucketOrds.find(key);
            if (bucketOrd<0) {
                throw new AggregationExecutionException("Inconsistency in histogram bucket reduction");
            }
            long docCount = counts.get(bucketOrd);
            if (docCount<minDocCount) {
                continue;
            }
            // empty aggregations
            listOfBuckets.add(new InternalHistogramBucket.Bucket(key, docCount, format, new InternalAggregations(Collections.emptyList())));
        };

        // on complete eventuellement jusqu'à maxBound
        if (minDocCount==0) {
            while (nextExpectedKey < extendedBounds.getMax()) {
                listOfBuckets.add(new InternalHistogramBucket.Bucket(nextExpectedKey, 0, format,
                        new InternalAggregations(Collections.emptyList())));
                nextExpectedKey += interval;
            }
        }

        logger.debug("END date histogram bucket reduction. Now ordering with { "+order.key()+" : "+(order.asc()?"asc":"desc")+" }");

        // et j'ai trié, trié-hé
        if (order == InternalOrder.KEY_ASC || context.isFinalReduce() == false) {
            // nothing to do, data are already sorted
        } else if (order == InternalOrder.KEY_DESC) {
            // we just need to reverse here...
            List<InternalHistogramBucket.Bucket> reverse = new ArrayList<>(listOfBuckets);
            Collections.reverse(reverse);
            listOfBuckets = reverse;
        } else {
            // sorted by sub-aggregation, need to fall back to a costly n*log(n) sort
            CollectionUtil.introSort(listOfBuckets, order.comparator());
        }

        logger.debug("END ordering date histogram");

        return new InternalDateHistogramBucket(name(),listOfBuckets,interval,minDocCount,format, Collections.emptyList(),metaData());
    }

    private Rounding createRounding() {
        Rounding.Builder tzRoundingBuilder;
        if (dateHistogramInterval != null) {
            DateTimeUnit dateTimeUnit = DATE_FIELD_UNITS.get(dateHistogramInterval.toString());
            if (dateTimeUnit != null) {
                tzRoundingBuilder = Rounding.builder(dateTimeUnit);
            } else {
                // the interval is a time value?
                tzRoundingBuilder = Rounding.builder(
                        TimeValue.parseTimeValue(dateHistogramInterval.toString(), null, getClass().getSimpleName() + ".interval"));
            }
        } else {
            // the interval is an integer time value in millis?
            tzRoundingBuilder = Rounding.builder(TimeValue.timeValueMillis(interval));
        }
        Rounding rounding = tzRoundingBuilder.build();
        interval = rounding.nextRoundingValue(0);
        return rounding;
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeOptionalWriteable(dateHistogramInterval);
        out.writeLong(interval);
        out.writeLong(offset);
        out.writeVLong(minDocCount);
        out.writeNamedWriteable(format);
        out.writeOptionalWriteable(extendedBounds);
        InternalOrder.Streams.writeOrder(order,out);
        out.writeGenericValue(bucketsPathsMap);
    }

    @Override
    public String getWriteableName() {
        return DateHistogramBucketAggregationBuilder.NAME;
    }
}
