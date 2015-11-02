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

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.RAMDirectory;

@SuppressWarnings("serial")
public abstract class LuceneAnalyzer extends TextDatumAnalyzer {

    private transient Directory _ramDir;
    private transient Analyzer _analyzer; 
    
    public LuceneAnalyzer() {
        this(true);
    }
    
    public LuceneAnalyzer(boolean useCommonWordsList) {
        _ramDir = null;
        _analyzer = null;
    }
    
    private synchronized void init() {
        if (_ramDir == null) {
            _ramDir = new RAMDirectory();
        }
        
        if (_analyzer == null) {
            _analyzer = createAnalyzer();
        }
    }
    
    abstract public Analyzer createAnalyzer();
    
    /**
     * @param contentText input text to be parsed into terms
     * @return salient terms in order of appearance
     * (or null if this content should be ignored)
     */
    public List<String> getTermList(String contentText) {
        init();
        List<String> result = new ArrayList<String>(contentText.length() / 10);
        
		try {
			TokenStream stream = _analyzer.tokenStream("content",
					new StringReader(contentText));
			CharTermAttribute termAtt = (CharTermAttribute) stream
					.addAttribute(CharTermAttribute.class);

			stream.reset();
			while (stream.incrementToken()) {
				if (termAtt.length() > 0) {
					String term = termAtt.toString();
					// Here we skip runs of position increment markers created
					// by the ShingleFilter for stop words because they skew
					// the clustering/liblinear analysis.
					if (!term.matches("(_ )*_")) {
						result.add(term);
					}
				}
			}
			stream.end();
			stream.close();
		} catch (IOException e) {
			throw new RuntimeException("Impossible error", e);
		}

        return result;
    }
    
    /* (non-Javadoc)
     * @see com.bixolabs.classify.analyzer.TextDatumAnalyzer#getTermMap(java.lang.String)
     */
    @Override
    public Map<String, Integer> getTermMap(String contentText) {
        List<String> termList = getTermList(contentText);
        Map<String, Integer> result = new HashMap<String, Integer>();
        for (String term : termList) {
            Integer termCount = result.get(term);
            if (termCount == null) {
                termCount = 0;
            }
            
            result.put(term, ++termCount);
        }
        
        return result;
    }
    
}
