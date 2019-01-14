/*
 *  Copyright 2018 sOftbridge Technology
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  see the License for the specific language governing permissions and
 *  limitations under the License.
 */

package com.softbridge.elasticsearch.plugins.pipeline.histogrambucket;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.index.query.MatchAllQueryBuilder;
import org.elasticsearch.search.aggregations.bucket.SingleBucketAggregation;
import org.elasticsearch.search.aggregations.bucket.filter.FilterAggregationBuilder;
import org.elasticsearch.search.aggregations.bucket.terms.StringTerms;
import org.elasticsearch.search.aggregations.bucket.terms.TermsAggregationBuilder;
import org.elasticsearch.search.aggregations.metrics.min.MinAggregationBuilder;
import org.elasticsearch.search.aggregations.support.ValueType;
import org.junit.Assert;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.elasticsearch.core.ElasticsearchTemplate;
import org.springframework.data.elasticsearch.core.query.IndexQuery;
import org.springframework.data.elasticsearch.core.query.NativeSearchQueryBuilder;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.TestExecutionListeners;
import org.springframework.test.context.junit4.rules.SpringClassRule;
import org.springframework.test.context.junit4.rules.SpringMethodRule;
import org.springframework.test.context.support.DependencyInjectionTestExecutionListener;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.Month;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by smazet on 24/04/18.
 */
@RunWith(com.carrotsearch.randomizedtesting.RandomizedRunner.class)
//@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = SpringTestsConfiguration.class)
@TestExecutionListeners(value = DependencyInjectionTestExecutionListener.class,
        mergeMode = TestExecutionListeners.MergeMode.MERGE_WITH_DEFAULTS)
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
public class HistogramBucketTestApp {

    private static final Logger logger = LogManager.getLogger(HistogramBucketTestApp.class);

    private static boolean isInitialized = false;

    @ClassRule
    public static final SpringClassRule SPRING_CLASS_RULE = new SpringClassRule();

    @Rule
    public final SpringMethodRule springMethodRule = new SpringMethodRule();

    @Autowired
    private ElasticsearchTemplate elasticsearchTemplate;

    private Map<String, String> histogramBucketsPathMap;

    @Before
    public void runBefore() {

        histogramBucketsPathMap = new HashMap<>();
        histogramBucketsPathMap.put("value", "bydoc>value");

        if (isInitialized) return;
        logger.info("Initializing database");

        if (!elasticsearchTemplate.indexExists(TestDocument.class)) {
            elasticsearchTemplate.createIndex(TestDocument.class);

            List<IndexQuery> docs = new ArrayList<>();
            addDocument(docs, new TestDocument("vehicule1", "car", 1200., toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 12, 0))));
            addDocument(docs, new TestDocument("vehicule2", "car", 2200., toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 13, 0))));
            addDocument(docs, new TestDocument("vehicule3", "truck", 1200., toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 15, 0))));
            addDocument(docs, new TestDocument("vehicule4", "truck", 2200., toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 17, 0))));
            elasticsearchTemplate.bulkIndex(docs);

            // ES takes some time to index data
            logger.warn("If tests failed, try launching tests again, give ES a chance to index docs.");

        }
        isInitialized = true;
    }

    private void addDocument(List<IndexQuery> docs, TestDocument document) {
        IndexQuery indexQuery = new IndexQuery();
        indexQuery.setObject(document);
        indexQuery.setType("");
        docs.add(indexQuery);
    }

    private Date toDate(LocalDateTime ldt) {
        return Date.from(ldt.atZone(ZoneId.systemDefault()).toInstant());
    }

    @Test
    public void testHistogramDefaultValues() {

        logger.info("Testing histogram_bucket default values.");

        HistogramBucketAggregationBuilder theHistogram = new HistogramBucketAggregationBuilder("theHistogram", histogramBucketsPathMap);

        Results results = getPipelineHistogramResults(theHistogram);

        Assert.assertEquals(4, results.getSearchHits().getTotalHits());
        Assert.assertEquals("sterms", results.getAggregations().asMap().get("bycat").getType());

        StringTerms resultsByCategory = (StringTerms) results.getAggregations().asMap().get("bycat");

        Assert.assertEquals(2, resultsByCategory.getBuckets().size());
        for (StringTerms.Bucket bucket : resultsByCategory.getBuckets()) {
            HistogramBucket histogramBucket = (HistogramBucket) bucket.getAggregations().asMap().get("theHistogram");
            // default interval is 100
            Assert.assertEquals(11, histogramBucket.getBuckets().size());
            Assert.assertEquals(1, histogramBucket.getBuckets().get(0).getDocCount());
            for (int i = 1; i < 10; i++) {
                Assert.assertEquals(0, histogramBucket.getBuckets().get(i).getDocCount());
            }
            Assert.assertEquals(1, histogramBucket.getBuckets().get(10).getDocCount());
        }

    }

    @Test
    public void testHistogramMinDocCount() {

        logger.info("Testing histogram_bucket min doc count.");

        HistogramBucketAggregationBuilder theHistogram = new HistogramBucketAggregationBuilder("theHistogram", histogramBucketsPathMap)
                .minDocCount(1);

        Results results = getPipelineHistogramResults(theHistogram);

        Assert.assertEquals(4, results.getSearchHits().getTotalHits());
        Assert.assertEquals("sterms", results.getAggregations().asMap().get("bycat").getType());

        StringTerms resultsByCategory = (StringTerms) results.getAggregations().asMap().get("bycat");

        Assert.assertEquals(2, resultsByCategory.getBuckets().size());
        for (StringTerms.Bucket bucket : resultsByCategory.getBuckets()) {
            HistogramBucket histogramBucket = (HistogramBucket) bucket.getAggregations().asMap().get("theHistogram");
            Assert.assertEquals(2, histogramBucket.getBuckets().size());
            Assert.assertEquals(1, ((HistogramBucket.Bucket) histogramBucket.getBuckets().get(0)).getDocCount());
            Assert.assertEquals(1, ((HistogramBucket.Bucket) histogramBucket.getBuckets().get(1)).getDocCount());
        }

    }

    @Test
    public void testHistogramIntervalAndOrder() {

        logger.info("Testing histogram_bucket interval and order keywords.");

        HistogramBucketAggregationBuilder theHistogram = new HistogramBucketAggregationBuilder("theHistogram", histogramBucketsPathMap)
                .interval(50.0).order(HistogramBucket.Order.KEY_DESC);

        Results results = getPipelineHistogramResults(theHistogram);

        Assert.assertEquals(4, results.getSearchHits().getTotalHits());
        Assert.assertEquals("sterms", results.getAggregations().asMap().get("bycat").getType());

        StringTerms resultsByCategory = (StringTerms) results.getAggregations().asMap().get("bycat");

        Assert.assertEquals(2, resultsByCategory.getBuckets().size());
        for (StringTerms.Bucket bucket : resultsByCategory.getBuckets()) {
            HistogramBucket histogramBucket = (HistogramBucket) bucket.getAggregations().asMap().get("theHistogram");
            Assert.assertEquals(21, histogramBucket.getBuckets().size());

            Assert.assertEquals(1, histogramBucket.getBuckets().get(0).getDocCount());
            Assert.assertEquals(2200.0, (Double) histogramBucket.getBuckets().get(0).getKey(), 0.0);

            Assert.assertEquals(1, histogramBucket.getBuckets().get(20).getDocCount());
            Assert.assertEquals(1200.0, (Double) histogramBucket.getBuckets().get(20).getKey(), 0.0);

            for (int i = 1; i < 20; i++) {
                Assert.assertEquals(0, histogramBucket.getBuckets().get(i).getDocCount());
            }
        }

    }


    @Test
    public void testDateHistogram() {

        logger.info("Testing date_histogram_bucket interval keyword.");

        DateHistogramBucketAggregationBuilder theDateHistogram = new DateHistogramBucketAggregationBuilder("theDateHistogram",
                histogramBucketsPathMap).interval(3600000);

        Results results = getPipelineHistogramResults(theDateHistogram);

        Assert.assertEquals(4, results.getSearchHits().getTotalHits());
        Assert.assertEquals("filter", results.getAggregations().asMap().get("alldocs").getType());

        SingleBucketAggregation allDocs = (SingleBucketAggregation) results.getAggregations().asMap().get("alldocs");

        DateHistogramBucket dateHistogram = (DateHistogramBucket) allDocs.getAggregations().asMap().get("theDateHistogram");

        Assert.assertEquals(6, dateHistogram.getBuckets().size());

        Assert.assertEquals(1, dateHistogram.getBuckets().get(0).getDocCount());
        Assert.assertEquals(toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 12, 0)),
                Date.from(Instant.ofEpochMilli(((Double) dateHistogram.getBuckets().get(0).getKey()).longValue())));

        Assert.assertEquals(1, dateHistogram.getBuckets().get(1).getDocCount());
        Assert.assertEquals(toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 13, 0)),
                Date.from(Instant.ofEpochMilli(((Double) dateHistogram.getBuckets().get(1).getKey()).longValue())));

        Assert.assertEquals(0, dateHistogram.getBuckets().get(2).getDocCount());
        Assert.assertEquals(toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 14, 0)),
                Date.from(Instant.ofEpochMilli(((Double) dateHistogram.getBuckets().get(2).getKey()).longValue())));

        Assert.assertEquals(1, dateHistogram.getBuckets().get(3).getDocCount());
        Assert.assertEquals(toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 15, 0)),
                Date.from(Instant.ofEpochMilli(((Double) dateHistogram.getBuckets().get(3).getKey()).longValue())));

        Assert.assertEquals(0, dateHistogram.getBuckets().get(4).getDocCount());
        Assert.assertEquals(toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 16, 0)),
                Date.from(Instant.ofEpochMilli(((Double) dateHistogram.getBuckets().get(4).getKey()).longValue())));

        Assert.assertEquals(1, dateHistogram.getBuckets().get(5).getDocCount());
        Assert.assertEquals(toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 17, 0)),
                Date.from(Instant.ofEpochMilli(((Double) dateHistogram.getBuckets().get(5).getKey()).longValue())));

    }

    @Test
    public void testDateHistogramMinDocCountAndOrder() {

        logger.info("Testing date_histogram_bucket min doc count and order keywords.");

        DateHistogramBucketAggregationBuilder theDateHistogram =
                new DateHistogramBucketAggregationBuilder("theDateHistogram", histogramBucketsPathMap)
                .interval(3600000).minDocCount(1).order(HistogramBucket.Order.KEY_DESC);

        Results results = getPipelineHistogramResults(theDateHistogram);

        Assert.assertEquals(4, results.getSearchHits().getTotalHits());
        Assert.assertEquals("filter", results.getAggregations().asMap().get("alldocs").getType());

        SingleBucketAggregation allDocs = (SingleBucketAggregation) results.getAggregations().asMap().get("alldocs");

        DateHistogramBucket dateHistogram = (DateHistogramBucket) allDocs.getAggregations().asMap().get("theDateHistogram");

        Assert.assertEquals(4, dateHistogram.getBuckets().size());

        Assert.assertEquals(1, dateHistogram.getBuckets().get(0).getDocCount());
        Assert.assertEquals(toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 17, 00)),
                Date.from(Instant.ofEpochMilli(((Double) dateHistogram.getBuckets().get(0).getKey()).longValue())));

        Assert.assertEquals(1, dateHistogram.getBuckets().get(1).getDocCount());
        Assert.assertEquals(toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 15, 00)),
                Date.from(Instant.ofEpochMilli(((Double) dateHistogram.getBuckets().get(1).getKey()).longValue())));

        Assert.assertEquals(1, dateHistogram.getBuckets().get(2).getDocCount());
        Assert.assertEquals(toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 13, 00)),
                Date.from(Instant.ofEpochMilli(((Double) dateHistogram.getBuckets().get(2).getKey()).longValue())));

        Assert.assertEquals(1, dateHistogram.getBuckets().get(3).getDocCount());
        Assert.assertEquals(toDate(LocalDateTime.of(2015, Month.AUGUST, 31, 12, 00)),
                Date.from(Instant.ofEpochMilli(((Double) dateHistogram.getBuckets().get(3).getKey()).longValue())));


    }

    private Results getPipelineHistogramResults(HistogramBucketAggregationBuilder theHistogram) {
        NativeSearchQueryBuilder nsqb;
        nsqb = buildTestHistogramQuery(theHistogram);
        return elasticsearchTemplate.query(nsqb.build(), response -> {
            Results results = new Results();
            results.setAggregations(response.getAggregations());
            results.setSearchHits(response.getHits());
            return results;
        });
    }

    private Results getPipelineHistogramResults(DateHistogramBucketAggregationBuilder theHistogram) {
        NativeSearchQueryBuilder nsqb;
        nsqb = buildTestDateHistogramQuery(theHistogram);
        return elasticsearchTemplate.query(nsqb.build(), response -> {
            Results results = new Results();
            results.setAggregations(response.getAggregations());
            results.setSearchHits(response.getHits());
            return results;
        });
    }

    private NativeSearchQueryBuilder buildTestHistogramQuery(HistogramBucketAggregationBuilder histogramBucketBuilder) {
        TermsAggregationBuilder bycat = new TermsAggregationBuilder("bycat", ValueType.STRING).field("category.keyword");

        TermsAggregationBuilder byDoc = new TermsAggregationBuilder("bydoc", ValueType.STRING).field("id.keyword");

        MinAggregationBuilder perDocValue = new MinAggregationBuilder("value").field("doubleValue");
        bycat.subAggregation(byDoc);
        byDoc.subAggregation(perDocValue);
        bycat.subAggregation(histogramBucketBuilder);

        return new NativeSearchQueryBuilder().withIndices("enhanced_aggregations_tests")
                .withQuery(new MatchAllQueryBuilder())
                .addAggregation(bycat);
    }

    private NativeSearchQueryBuilder buildTestDateHistogramQuery(DateHistogramBucketAggregationBuilder dateHistogramBuilder) {
        FilterAggregationBuilder allDocs = new FilterAggregationBuilder("alldocs", new MatchAllQueryBuilder());

        TermsAggregationBuilder byDoc = new TermsAggregationBuilder("bydoc", ValueType.STRING).field("id.keyword");

        MinAggregationBuilder perDocValue = new MinAggregationBuilder("value").field("date");
        allDocs.subAggregation(byDoc);
        byDoc.subAggregation(perDocValue);
        allDocs.subAggregation(dateHistogramBuilder);

        return new NativeSearchQueryBuilder().withIndices("enhanced_aggregations_tests")
                .withQuery(new MatchAllQueryBuilder())
                .addAggregation(allDocs);
    }


}
