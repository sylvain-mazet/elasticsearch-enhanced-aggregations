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

import org.springframework.data.elasticsearch.annotations.Document;
import org.springframework.data.elasticsearch.annotations.Mapping;
import org.springframework.data.elasticsearch.annotations.Setting;

import java.util.Date;

/**
 * Created by smazet on 24/04/18.
 */
@Document(indexName = "enhanced_aggregations_tests", type = "testDocument", shards = 1, replicas = 0,
        typeV6="testDocument"
)
@Mapping(mappingPath = "test-document-mapping.json")
@Setting(settingPath = "test-document-settings.json")
public class TestDocument {
    TestDocument(String id, String category, Double doubleValue, Date date) {
        this.id = id;
        this.category = category;
        this.doubleValue = doubleValue;
        this.date = date;
    }

    private String id;

    private String category;

    private Double doubleValue;

    private Date date;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCategory() {
        return category;
    }

    public void setCategory(String category) {
        this.category = category;
    }

    public Double getDoubleValue() {
        return doubleValue;
    }

    public void setDoubleValue(Double doubleValue) {
        this.doubleValue = doubleValue;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }
}
