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

import java.io.IOException;
import java.util.Comparator;
import java.util.Objects;

/**
 * Created by smazet on 09/02/18.
 */
class InternalOrder extends InternalHistogramBucket.Order {
    private final byte id;
    private final String key;
    private final boolean asc;
    private final Comparator<InternalHistogramBucket.Bucket> comparator;

    InternalOrder(byte id, String key, boolean asc, Comparator<InternalHistogramBucket.Bucket> comparator) {
        this.id = id;
        this.key = key;
        this.asc = asc;
        this.comparator = comparator;
    }

    byte id() {
        return id;
    }

    String key() {
        return key;
    }

    boolean asc() {
        return asc;
    }

    @Override
    Comparator<InternalHistogramBucket.Bucket> comparator() {
        return comparator;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return builder.startObject().field(key, asc ? "asc" : "desc").endObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, key, asc);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        InternalOrder other = (InternalOrder) obj;
        return Objects.equals(id, other.id)
                && Objects.equals(key, other.key)
                && Objects.equals(asc, other.asc);
    }

    static class Streams {

        /**
         * Writes the given order to the given output (based on the id of the order).
         */
        public static void writeOrder(InternalOrder order, StreamOutput out) throws IOException {
            out.writeByte(order.id());
        }

        /**
         * Reads an order from the given input (based on the id of the order).
         *
         */
        public static InternalOrder readOrder(StreamInput in) throws IOException {
            byte id = in.readByte();
            switch (id) {
                case 1:
                    return (InternalOrder) InternalHistogramBucket.Order.KEY_ASC;
                case 2:
                    return (InternalOrder) InternalHistogramBucket.Order.KEY_DESC;
                case 3:
                    return (InternalOrder) InternalHistogramBucket.Order.COUNT_ASC;
                case 4:
                    return (InternalOrder) InternalHistogramBucket.Order.COUNT_DESC;
                default:
                    throw new RuntimeException("unknown histogram bucket order");
            }
        }
    }
}
