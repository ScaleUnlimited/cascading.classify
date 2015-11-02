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

import com.scaleunlimited.cascading.BaseDatum;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;


@SuppressWarnings("serial")
public class ThresholdDatum extends BaseDatum {
    public static final String THRESHOLD_FN = fieldName(ThresholdDatum.class, "threshold");

    public static final Fields FIELDS = new Fields(THRESHOLD_FN);
    
    public ThresholdDatum(Fields fields) {
        super(fields);
    }
    
    public ThresholdDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }
    
    public ThresholdDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
    }
    
    public ThresholdDatum(double threshold) {
        super(FIELDS);
        setThreshold(threshold);
    }
    
    public double getThreshold() {
        return _tupleEntry.getDouble(THRESHOLD_FN);
    }
    
    public void setThreshold(double threshold) {
        _tupleEntry.set(THRESHOLD_FN, threshold);
    }
    
}
