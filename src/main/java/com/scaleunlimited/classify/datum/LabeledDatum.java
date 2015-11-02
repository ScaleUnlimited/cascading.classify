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

import com.scaleunlimited.cascading.PayloadDatum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

@SuppressWarnings("serial")
public class LabeledDatum extends PayloadDatum {
    public static final String LABEL_FN = fieldName(LabeledDatum.class, "label");

    public static final Fields FIELDS =
        PayloadDatum.FIELDS.append(new Fields(LABEL_FN));
    
    public LabeledDatum(Fields fields) {
        super(fields);
    }
    
    public LabeledDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
    }
    
    public LabeledDatum(String label) {
        super(FIELDS);
        setLabel(label);
    }

    public String getLabel() {
        return _tupleEntry.getString(LABEL_FN);
    }

    public void setLabel(String label) {
        _tupleEntry.set(LABEL_FN, label);
    }
}
