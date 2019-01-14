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
import org.elasticsearch.common.util.LongArray;
import org.elasticsearch.common.util.LongHash;
import org.elasticsearch.search.DocValueFormat;
import org.elasticsearch.search.aggregations.Aggregation;
import org.elasticsearch.search.aggregations.AggregationExecutionException;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.aggregations.InternalAggregation;
import org.elasticsearch.search.aggregations.InternalAggregations;
import org.elasticsearch.search.aggregations.InternalMultiBucketAggregation;
import org.elasticsearch.search.aggregations.pipeline.BucketHelpers;
import org.elasticsearch.search.aggregations.pipeline.SiblingPipelineAggregator;
import org.elasticsearch.search.aggregations.support.AggregationPath;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by smazet on 05/02/18.
 */
public class HistogramBucketAggregator extends SiblingPipelineAggregator {

    private Logger logger = LogManager.getLogger(HistogramBucketAggregator.class);

    private final BucketHelpers.GapPolicy gapPolicy = BucketHelpers.GapPolicy.SKIP;
    private final Map<String, String> bucketsPathsMap;
    private final double interval;
    private final double offset;
    private final double minBound;
    private final double maxBound;
    private final long minDocCount;
    private final InternalOrder order;
    private final DocValueFormat formatter;

    HistogramBucketAggregator(String name, double interval, double offset, double minBound, double maxBound,
                              long minDocCount, InternalOrder order, DocValueFormat formatter,
                              Map<String, String> bucketsPathsMap,
                              Map<String, Object> metadata) {
        super(name, bucketsPathsMap.values().toArray(new String[bucketsPathsMap.size()]), metadata);
        this.bucketsPathsMap = bucketsPathsMap;
        this.interval = interval;
        this.offset = offset;
        this.minBound = minBound;
        this.maxBound = maxBound;
        this.minDocCount = minDocCount;
        this.order = order;
        this.formatter = formatter;
    }

    @SuppressWarnings("unchecked")
    public HistogramBucketAggregator(StreamInput in) throws IOException {
        super(in);
        this.interval = in.readDouble();
        this.offset = in.readDouble();
        this.minBound = in.readDouble();
        this.maxBound = in.readDouble();
        this.minDocCount = in.readVLong();
        this.order = InternalOrder.Streams.readOrder(in);
        this.formatter = in.readNamedWriteable(DocValueFormat.class);
        this.bucketsPathsMap = (Map<String, String>)in.readGenericValue();
    }

    @Override
    protected void doWriteTo(StreamOutput out) throws IOException {
        out.writeDouble(interval);
        out.writeDouble(offset);
        out.writeDouble(minBound);
        out.writeDouble(maxBound);
        out.writeVLong(minDocCount);
        InternalOrder.Streams.writeOrder(order,out);
        out.writeNamedWriteable(formatter);
        out.writeGenericValue(bucketsPathsMap);
    }

    @Override
    public InternalAggregation doReduce(Aggregations aggregations, InternalAggregation.ReduceContext context) {
        List<String> bucketsPath = AggregationPath.parse(bucketsPaths()[0]).getPathElementsAsStringList();
        logger.debug("START histogram bucket reduction");

        Map<Double,InternalHistogramBucket.Bucket> newBuckets = new HashMap<>(1);

        LongHash bucketOrds = new LongHash(1,context.bigArrays());
        PriorityQueue<Double> pq = null;
        LongArray counts = null;

        for (Aggregation aggregation : aggregations) {
            if (aggregation.getName().equals(bucketsPath.get(0))) {
                bucketsPath = bucketsPath.subList(1, bucketsPath.size());
                InternalMultiBucketAggregation<?, ?> multiBucketsAgg = (InternalMultiBucketAggregation<?, ?>) aggregation;
                List<? extends InternalMultiBucketAggregation.InternalBucket> buckets = multiBucketsAgg.getBuckets();

                counts = context.bigArrays().newLongArray(buckets.size());

                pq = new PriorityQueue<Double>(buckets.size()) {
                    @Override
                    protected boolean lessThan(Double a, Double b) {
                        return a < b;
                    }
                };

                for (InternalMultiBucketAggregation.InternalBucket bucket : buckets) {
                    Double bucketValue = BucketHelpers.resolveBucketValue(multiBucketsAgg, bucket, bucketsPath, gapPolicy);

                    if (bucketValue != null && !Double.isNaN(bucketValue)) {

                        Double key = Math.floor(( bucketValue - offset )/ interval);

                        long bucketOrd = bucketOrds.add(Double.doubleToLongBits(key));

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

        // sort buckets as in ES InternalHistogram
        List<InternalHistogramBucket.Bucket> listOfBuckets = new ArrayList<>(newBuckets.size());
        double nextExpectedKey = Math.floor((minBound - offset)/interval) * interval + offset;
        do {
            Double roundKey = pq.pop();
            if (null==roundKey) {
                break;
            }
            double key = roundKey * interval + offset;

            if (minDocCount == 0 && nextExpectedKey < key) {
                while (nextExpectedKey < key) {
                    listOfBuckets.add(new InternalHistogramBucket.Bucket(nextExpectedKey, 0, formatter,
                            new InternalAggregations(Collections.emptyList())));
                    nextExpectedKey += interval;
                }
            }
            nextExpectedKey = key + interval;

            long bucketOrd = bucketOrds.find(Double.doubleToLongBits(roundKey));
            if (bucketOrd<0) {
                throw new AggregationExecutionException("Inconsistency in histogram bucket reduction");
            }
            long docCount = counts.get(bucketOrd);
            if (docCount<minDocCount) {
                continue;
            }
            // empty aggregations
            listOfBuckets.add(new InternalHistogramBucket.Bucket(key, docCount, formatter,
                    new InternalAggregations(Collections.emptyList())));
        } while (pq.size()>0);

        // on complete eventuellement jusqu'à maxBound
        if (minDocCount==0) {
            while (nextExpectedKey < maxBound) {
                    listOfBuckets.add(new InternalHistogramBucket.Bucket(nextExpectedKey, 0, formatter,
                            new InternalAggregations(Collections.emptyList())));
                    nextExpectedKey += interval;
                }
        }

        logger.debug("END histogram bucket reduction. Now ordering with { "+order.key()+" : "+(order.asc()?"asc":"desc")+" }");

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

        logger.debug("END ordering histogram");

        // no pipeline aggregators
        return new InternalHistogramBucket(name(),listOfBuckets,interval,minDocCount,formatter,Collections.emptyList(),metaData());
    }

    @Override
    public String getWriteableName() {
        return HistogramBucketAggregationBuilder.NAME;
    }

}
