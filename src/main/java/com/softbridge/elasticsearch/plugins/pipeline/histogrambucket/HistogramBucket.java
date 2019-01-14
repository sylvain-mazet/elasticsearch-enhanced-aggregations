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

import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.search.aggregations.bucket.MultiBucketsAggregation;
import org.elasticsearch.search.aggregations.bucket.histogram.Histogram;

import java.util.Comparator;

/**
 * Created by smazet on 22/02/18.
 */
public interface HistogramBucket extends MultiBucketsAggregation {

    abstract class Order implements ToXContent {
        abstract Comparator<InternalHistogramBucket.Bucket> comparator();

        private static int compareKey(Histogram.Bucket b1, Histogram.Bucket b2) {
            if (b1 instanceof InternalHistogramBucket.Bucket) {
                return Double.compare(((InternalHistogramBucket.Bucket) b1).key, ((InternalHistogramBucket.Bucket) b2).key);
            } else {
                throw new IllegalStateException("Unexpected impl: " + b1.getClass());
            }
        }

        public static final Order KEY_ASC = new InternalOrder((byte) 1, "_key", true,
                new Comparator<InternalHistogramBucket.Bucket>() {
                    @Override
                    public int compare(InternalHistogramBucket.Bucket b1, InternalHistogramBucket.Bucket b2) {
                        return compareKey(b1, b2);
                    }
                });

        public static final Order KEY_DESC = new InternalOrder((byte) 2, "_key", false,
                new Comparator<InternalHistogramBucket.Bucket>() {
                    @Override
                    public int compare(InternalHistogramBucket.Bucket b1, InternalHistogramBucket.Bucket b2) {
                        return compareKey(b2, b1);
                    }
                });

        public static final Order COUNT_ASC = new InternalOrder((byte) 3, "_count", true,
                new Comparator<InternalHistogramBucket.Bucket>() {
                    @Override
                    public int compare(InternalHistogramBucket.Bucket b1, InternalHistogramBucket.Bucket b2) {
                        int cmp = Long.compare(b1.getDocCount(), b2.getDocCount());
                        if (cmp == 0) {
                            cmp = compareKey(b1, b2);
                        }
                        return cmp;
                    }
                });

        public static final Order COUNT_DESC = new InternalOrder((byte) 4, "_count", false,
                new Comparator<InternalHistogramBucket.Bucket>() {
                    @Override
                    public int compare(InternalHistogramBucket.Bucket b1, InternalHistogramBucket.Bucket b2) {
                        int cmp = Long.compare(b2.getDocCount(), b1.getDocCount());
                        if (cmp == 0) {
                            cmp = compareKey(b1, b2);
                        }
                        return cmp;
                    }
                });

    }
}
