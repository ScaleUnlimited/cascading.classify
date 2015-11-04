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

import com.scaleunlimited.cascading.PayloadDatum;
import com.scaleunlimited.classify.BaseModel;

/**
 * The classification information for a single {@link TermsDatum} (which itself
 * likely originated through the analysis of a single {@link TextDatum}).
 * Each document is assigned to exactly one classification (according to
 * the {@link BaseModel} subclass that performed the classification).
 * The classification set is defined during training, and each classification
 * is identified by a String label.
 * 
 * TODO CSc Support multiple classifications for a single DocDatum
 * (e.g., all those scoring above a threshold).
 */
@SuppressWarnings("serial")
public class DocDatum extends PayloadDatum {
    
    // TODO CSc Should we generalize this so that a document has an array of scores,
    // one for each of the model's labels?
    public static final String LABEL_FN = fieldName(DocDatum.class, "label");
    public static final String SCORE_FN = fieldName(DocDatum.class, "score");

    public static final Fields FIELDS =
        PayloadDatum.FIELDS.append(new Fields(  LABEL_FN,
                                                SCORE_FN));
    
    public DocDatum(Fields fields) {
        super(fields);
    }

    public DocDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }
    
    public DocDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
    }
    
    public DocDatum(//Vector attributes,
                    String label,
                    float score) {
        super(FIELDS);
        setLabel(label);
        setScore(score);
    }

    public String getLabel() {
        return _tupleEntry.getString(LABEL_FN);
    }

    public void setLabel(String label) {
        _tupleEntry.setString(LABEL_FN, label);
    }

    public float getScore() {
        return _tupleEntry.getFloat(SCORE_FN);
    }

    public void setScore(float score) {
        _tupleEntry.setFloat(SCORE_FN, score);
    }

}
