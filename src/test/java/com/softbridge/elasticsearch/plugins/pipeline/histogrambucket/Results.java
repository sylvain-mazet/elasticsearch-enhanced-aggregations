/*
 *  Copyright 2018 sOftbridge Technology
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  see the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.softbridge.elasticsearch.plugins.pipeline.histogrambucket;

import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;

/**
 * Created by smazet on 24/04/18.
 */
public class Results {
    private Aggregations aggregations;
    private SearchHits searchHits;

    Aggregations getAggregations() {
        return aggregations;
    }

    void setAggregations(Aggregations aggregations) {
        this.aggregations = aggregations;
    }

    SearchHits getSearchHits() {
        return searchHits;
    }

    void setSearchHits(SearchHits searchHits) {
        this.searchHits = searchHits;
    }
}
