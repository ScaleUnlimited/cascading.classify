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
package com.scaleunlimited.classify.vectors;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.mahout.math.RandomAccessSparseVector;
import org.apache.mahout.math.Vector;

public class VectorUtils {

    public static Map<String, Integer> makeTermMap(List<String> docTerms) {
        Map<String, Integer> termMap = new HashMap<String, Integer>();
        for (String term : docTerms) {
            Integer curCount = termMap.get(term);
            if (curCount == null) {
                termMap.put(term, new Integer(1));
            } else {
                termMap.put(term, curCount + 1);
            }
        }

        return termMap;
    }
    
    public static Vector makeVector(List<String> uniqueTerms, List<String> docTerms) {
        return makeVector(uniqueTerms, makeTermMap(docTerms));
    }

    /**
     * Create a vector from the (sorted) list of unique terms, and the map of terms/counts
     * for a document
     * 
     * @param terms
     * @param docTerms
     * @return vector of term frequencies
     */
    public static Vector makeVector(List<String> terms, Map<String, Integer> docTerms) {
        Vector result = new RandomAccessSparseVector(terms.size());
        
        for (String term : docTerms.keySet()) {
            int index = Collections.binarySearch(terms, term);
            if (index < 0) {
                // This can happen when we're making a vector for classification
                // result, since docTerms contains terms from a random doc, but
                // terms has the terms from cluster generation.
            } else {
                int value = docTerms.get(term);
                result.setQuick(index, value);
            }
        }
        
        return result;
    }

    public static Vector makeVectorDouble(List<String> featuresList, Map<String, Double> featureMap) {
        Vector result = new RandomAccessSparseVector(featuresList.size());
        
        for (String term : featureMap.keySet()) {
            int index = Collections.binarySearch(featuresList, term);
            if (index < 0) {
                // This can happen when we're making a vector for classification
                // result, since docTerms contains terms from a random doc, but
                // terms has the terms from cluster generation.
            } else {
                double value = featureMap.get(term);
                result.setQuick(index, value);
            }
        }
        return result;
    }

    public static Vector makeExtraVector(List<String> terms, Map<String, Integer> docTerms) {
        List<String> extraTerms = new ArrayList<String>();
        
        for (String term : docTerms.keySet()) {
            int index = Collections.binarySearch(terms, term);
            if (index < 0) {
                extraTerms.add(term);
            }
        }
        
        Vector result = new RandomAccessSparseVector(extraTerms.size());
        Collections.sort(extraTerms);
        
        int index = 0;
        for (String extraTerm : extraTerms) {
            int value = docTerms.get(extraTerm);
            if (value != 0) {
                result.setQuick(index++, value);
            }
        }

        return result;
    }

    public static Vector appendVectors(Vector baseVector, Vector extraVector) {
        int baseSize = baseVector.size();
        Vector result = new RandomAccessSparseVector(baseSize + extraVector.size());

        for (int i = 0; i < baseSize; i++) {
            double value = baseVector.getQuick(i);
            if (value != 0.0) {
                result.setQuick(i, value);
            }
        }
        
        for (int i = 0; i < extraVector.size(); i++) {
            double value = extraVector.getQuick(i);
            if (value != 0.0) {
                result.setQuick(baseSize + i, value);
            }
        }
        
        return result;
    }

    public static Vector extendVector(Vector v, int extraSize) {
        if (extraSize == 0) {
            return v;
        }
        
        int baseSize = v.size();
        Vector result = new RandomAccessSparseVector(baseSize + extraSize);
        for (int i = 0; i < baseSize; i++) {
            double value = v.getQuick(i);
            if (value != 0.0) {
                result.setQuick(i, value);
            }
        }

        return result;
    }
    
    public static String dumpVector(Vector v) {
        StringBuffer result = new StringBuffer();
        result.append(String.format("Vector '%s': ", "<unknown>"));
        int baseSize = v.size();
        for (int i = 0; i < baseSize; i++) {
            double component = v.getQuick(i);
            if (component != 0.0) {
                result.append(String.format("%d => %f, ", i, component));
            }
        }
        return result.toString();
    }
    
}
