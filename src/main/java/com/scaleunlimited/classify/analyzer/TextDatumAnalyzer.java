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
package com.scaleunlimited.classify.analyzer;

import java.util.Map;

import cascading.tuple.Tuple;

import com.scaleunlimited.classify.BaseModel;
import com.scaleunlimited.classify.datum.TermsDatum;
import com.scaleunlimited.classify.datum.TextDatum;

/**
 * Pre-processor of raw {@link TextDatum} documents, from which an array
 * of Strings and frequency counts is extracted and returned as a {@link TermsDatum}.
 * The latter then serve as input to a classification model ({@link BaseModel} subclass),
 * both as training documents and then documents to be classified by the trained model.
 */
@SuppressWarnings("serial")
public abstract class TextDatumAnalyzer implements IAnalyzer {

    /**
     * @param contentText input text to be parsed into terms
     * @return salient terms and their frequencies
     * (or null if this content should be ignored)
     */
    abstract public Map<String, Integer> getTermMap(String contentText);
    
    /**
     * @param tuple (TextDatum) contains input text to be parsed into terms
     * @return datum containing salient terms and their frequencies
     * (or null if this tuple should be ignored)
     */
    @Override
    public TermsDatum getTermsDatum(Tuple tuple) {
        TermsDatum result = null;
        TextDatum textDatum = new TextDatum(tuple);
        Map<String, Integer> termMap = getTermMap(textDatum.getContent());
        if (termMap != null) {
            result = new TermsDatum(termMap, textDatum.getLabel());
            result.setPayload(textDatum.getPayload());
        }
        return result;
    }
        
    /**
     * Initialize the newly constructed (e.g., recently deserialized) analyzer,
     * particularly its transient fields.
     */
    @Override
    public void reset() {
        // Base does nothing.
    }
    
    /**
     * Log any statistics that were collected during analysis.
     */
    @Override
    public void stats() {
        // Base does nothing.
    }
}
