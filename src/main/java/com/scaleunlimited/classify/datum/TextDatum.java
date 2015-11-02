/**
 * Copyright (c) 2009-2015 Scale Unlimited, Inc.
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
 */
package com.scaleunlimited.classify.datum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.classify.BaseModel;
import com.scaleunlimited.classify.analyzer.TextDatumAnalyzer;

/**
 * Raw text for a single document, ready for pre-processing by an analyzer
 * ({@link TextDatumAnalyzer} subclass) into a {@link TermsDatum}. The latter
 * is input to the classification model ({@link BaseModel}) subclass.
 */
@SuppressWarnings("serial")
public class TextDatum extends LabeledDatum {

    public static final String CONTENT_FN = fieldName(TextDatum.class, "content");

    public static final Fields FIELDS =
        LabeledDatum.FIELDS.append(new Fields(CONTENT_FN));
    
    public TextDatum(Fields fields) {
        super(fields);
    }

    public TextDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }

    public TextDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
    }
    
    public TextDatum(String content) {
        this(content, BaseModel.NOT_YET_LABELED);
    }

    public TextDatum(String content, String label) {
        super(FIELDS);
        super.setLabel(label);
        setContent(content);
    }

    public String getContent() {
        return _tupleEntry.getString(CONTENT_FN);
    }

    public void setContent(String content) {
        _tupleEntry.set(CONTENT_FN, content);
    }

}
