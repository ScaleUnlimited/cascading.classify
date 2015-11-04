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

import java.util.HashMap;
import java.util.Map;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.classify.BaseModel;
import com.scaleunlimited.classify.analyzer.TextDatumAnalyzer;

/**
 * Input to the classification model ({@link BaseModel} subclass)
 * for a single document (likely originating through the analysis of a
 * single {@link TextDatum} by an analyzer ({@link TextDatumAnalyzer} subclass).
 * Training documents should have non-null labels.
 */
@SuppressWarnings("serial")
public class TermsDatum extends LabeledDatum {

    public static final String TERMS_FN = fieldName(TermsDatum.class, "terms");
    public static final String TERM_COUNTS_FN = fieldName(TermsDatum.class, "termcounts");

    public static final Fields FIELDS =
        LabeledDatum.FIELDS.append(new Fields(  TERMS_FN,
                                                TERM_COUNTS_FN));
    
    public TermsDatum(Fields fields) {
        super(fields);
    }

    public TermsDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }

    public TermsDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
    }
    
    public TermsDatum(Map<String, Integer> termMap) {
        super(FIELDS);
        setTermMap(termMap);
        setLabel(BaseModel.NOT_YET_LABELED);
    }

    public TermsDatum(Map<String, Integer> termMap, String label) {
        super(FIELDS);
        super.setLabel(label);
        setTermMap(termMap);
    }

    public Map<String, Integer> getTermMap() {
        Tuple termsTuple = (Tuple)_tupleEntry.getObject(TERMS_FN);
        Tuple termCountsTuple = (Tuple)_tupleEntry.getObject(TERM_COUNTS_FN);
        int numTerms = termsTuple.size();
        Map<String, Integer> result = new HashMap<String, Integer>(numTerms);
        for (int i = 0; i < numTerms; i++) {
            result.put(termsTuple.getString(i), termCountsTuple.getInteger(i));
        }
        return result;
    }

    public void setTermMap(Map<String, Integer> termMap) {
        Tuple termsTuple = new Tuple();
        Tuple termCountsTuple = new Tuple();
        for (Map.Entry<String, Integer> entry : termMap.entrySet()) {
            termsTuple.add(entry.getKey());
            termCountsTuple.add(entry.getValue());
        }
        _tupleEntry.set(TERMS_FN, termsTuple);
        _tupleEntry.set(TERM_COUNTS_FN, termCountsTuple);
    }
}
