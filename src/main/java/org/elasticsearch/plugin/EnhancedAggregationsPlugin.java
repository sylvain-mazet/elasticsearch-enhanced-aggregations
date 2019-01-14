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
package org.elasticsearch.plugin;

import com.softbridge.elasticsearch.plugins.pipeline.histogrambucket.DateHistogramBucketAggregationBuilder;
import com.softbridge.elasticsearch.plugins.pipeline.histogrambucket.DateHistogramBucketAggregator;
import com.softbridge.elasticsearch.plugins.pipeline.histogrambucket.HistogramBucketAggregationBuilder;
import com.softbridge.elasticsearch.plugins.pipeline.histogrambucket.HistogramBucketAggregator;
import com.softbridge.elasticsearch.plugins.pipeline.histogrambucket.InternalDateHistogramBucket;
import com.softbridge.elasticsearch.plugins.pipeline.histogrambucket.InternalHistogramBucket;
import org.elasticsearch.plugins.Plugin;
import org.elasticsearch.plugins.SearchPlugin;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Created by smazet on 05/02/18.
 */
public class EnhancedAggregationsPlugin extends Plugin implements SearchPlugin {

    public EnhancedAggregationsPlugin() {
    }

    public String name() {return "enhanced-aggregations";}

    public String description() {
        return "Add histograms to pipeline aggregators.";
    }

    private static final List<PipelineAggregationSpec> specs;

    static {
        specs = new ArrayList<>();
        specs.add(new PipelineAggregationSpec(
                HistogramBucketAggregationBuilder.NAME,
                HistogramBucketAggregationBuilder::new,
                HistogramBucketAggregator::new,
                HistogramBucketAggregationBuilder::parse
        ).addResultReader(InternalHistogramBucket.NAME, InternalHistogramBucket::new));
        specs.add(new PipelineAggregationSpec(
                DateHistogramBucketAggregationBuilder.NAME,
                DateHistogramBucketAggregationBuilder::new,
                DateHistogramBucketAggregator::new,
                DateHistogramBucketAggregationBuilder::parse
        ).addResultReader(InternalDateHistogramBucket.NAME, InternalDateHistogramBucket::new));
    }

    @Override
    public List<PipelineAggregationSpec> getPipelineAggregations() {
        return Collections.unmodifiableList(specs);
    }

}
