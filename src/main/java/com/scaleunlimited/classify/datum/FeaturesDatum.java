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

/**
 * Input to the classification model ({@link BaseModel} subclass)
 * for a single document that already has computed feature values 
 * Training documents should have non-null labels.
 */
@SuppressWarnings("serial")
public class FeaturesDatum extends LabeledDatum {

    public static final String FEATURES_FN = fieldName(FeaturesDatum.class, "features");
    public static final String FEATURE_VALUES_FN = fieldName(FeaturesDatum.class, "featurevals");

    public static final Fields FIELDS =
        LabeledDatum.FIELDS.append(new Fields(  FEATURES_FN,
                                                FEATURE_VALUES_FN));
    
    public FeaturesDatum(Fields fields) {
        super(fields);
    }

    public FeaturesDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }

    public FeaturesDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
    }
    
    public FeaturesDatum(Map<String, Double> featureMap) {
        super(FIELDS);
        setFeatureMap(featureMap);
        setLabel(BaseModel.NOT_YET_LABELED);
    }

    public FeaturesDatum(Map<String, Double> featureMap, String label) {
        super(FIELDS);
        super.setLabel(label);
        setFeatureMap(featureMap);
    }

    public Map<String, Double> getFeatureMap() {
        Tuple featuresTuple = (Tuple)_tupleEntry.getObject(FEATURES_FN);
        Tuple featureValsTuple = (Tuple)_tupleEntry.getObject(FEATURE_VALUES_FN);
        int numFeatures = featuresTuple.size();
        Map<String, Double> result = new HashMap<String, Double>(numFeatures);
        for (int i = 0; i < numFeatures; i++) {
            result.put(featuresTuple.getString(i), featureValsTuple.getDouble(i));
        }
        return result;
    }

    public void setFeatureMap(Map<String, Double> featureMap) {
        Tuple featuresTuple = new Tuple();
        Tuple featureValsTuple = new Tuple();
        for (Map.Entry<String, Double> entry : featureMap.entrySet()) {
            featuresTuple.add(entry.getKey());
            featureValsTuple.add(entry.getValue());
        }
        _tupleEntry.setObject(FEATURES_FN, featuresTuple);
        _tupleEntry.setObject(FEATURE_VALUES_FN, featureValsTuple);
    }
}
