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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import junit.framework.Assert;

import org.junit.Test;

import com.scaleunlimited.classify.analyzer.WhitespaceAnalyzer;
import com.scaleunlimited.classify.datum.TermsDatum;
import com.scaleunlimited.classify.datum.TextDatum;


public class WhitespaceAnalyzerTest {

    @Test
    public void testAnalyzer() {
        String testText = "this is a test of the Lucene whitespace analyzer";
        TextDatum textDatum = new TextDatum(testText);
        WhitespaceAnalyzer analyzer = new WhitespaceAnalyzer();
        analyzer.reset();
        TermsDatum termsDatum = analyzer.getTermsDatum(textDatum.getTuple());
        
        List<String> terms = new ArrayList<String>();
        terms.addAll(termsDatum.getTermMap().keySet());
        Collections.sort(terms);
        
        Assert.assertTrue(Collections.binarySearch(terms, "this") >= 0);
        Assert.assertTrue(Collections.binarySearch(terms, "test") >= 0);
        Assert.assertTrue(Collections.binarySearch(terms, "Lucene") >= 0);
        Assert.assertTrue(Collections.binarySearch(terms, "analyzer") >= 0);
        
        // Analyzer shouldn't lower-case
        Assert.assertTrue(Collections.binarySearch(terms, "lucene") < 0);
    }
}
