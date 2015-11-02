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

import com.scaleunlimited.classify.analyzer.LuceneAnalyzer;
import com.scaleunlimited.classify.analyzer.ShingleAnalyzer;
import com.scaleunlimited.classify.datum.TermsDatum;
import com.scaleunlimited.classify.datum.TextDatum;


public class ShingleAnalyzerTest {

    @Test
    public void testAnalyzer() {
        String testText = "this is a test of the lucene analyzer which makes use of the shingle analyzer";
        TextDatum textDatum = new TextDatum(testText);
        LuceneAnalyzer lAnalyzer = new ShingleAnalyzer();
        lAnalyzer.reset();
        TermsDatum termsDatum = lAnalyzer.getTermsDatum(textDatum.getTuple());
        
        List<String> terms = new ArrayList<String>();
        terms.addAll(termsDatum.getTermMap().keySet());
        Collections.sort(terms);
        
        Assert.assertTrue(Collections.binarySearch(terms, "lucene") >= 0);
        Assert.assertTrue(Collections.binarySearch(terms, "lucene analyzer") >= 0);
        Assert.assertTrue(Collections.binarySearch(terms, "is a") < 0);
        Assert.assertTrue(Collections.binarySearch(terms, "the") < 0);
        Assert.assertTrue(Collections.binarySearch(terms, "the lucene") < 0);
    }

}
