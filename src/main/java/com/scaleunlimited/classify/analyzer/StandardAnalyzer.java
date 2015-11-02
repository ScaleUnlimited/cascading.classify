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
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@SuppressWarnings("serial")
public class StandardAnalyzer extends LuceneAnalyzer {
    private static final Logger LOGGER = LoggerFactory.getLogger(StandardAnalyzer.class);
    
    private static final String COMMON_WORDS_FILENAME = "common-words.txt";

    @Override
    public Analyzer createAnalyzer() {
        return createStandardAnalyzer(true);
    }

    public static Analyzer createStandardAnalyzer(boolean useFullStopWords) {
        Analyzer result;
        
        if (useFullStopWords) {
            InputStream fis = StandardAnalyzer.class.getResourceAsStream("/" + COMMON_WORDS_FILENAME);
            if (fis == null) {
                String message = String.format(  "Unable to find common words file resource '%s'",
                                COMMON_WORDS_FILENAME);
                throw new RuntimeException(message);
            }

            Reader in = new InputStreamReader(fis);
            try {
                result = new org.apache.lucene.analysis.standard.StandardAnalyzer(Version.LUCENE_CURRENT, in);
            } catch (Exception e) {
                String message =
                    String.format("Error while reading from common words file '%s'", COMMON_WORDS_FILENAME);
                throw new RuntimeException(message, e);
            } finally {
                try {
                    if (in != null) {
                        in.close();
                    }
                    if (fis != null) {
                        fis.close();
                    }
                } catch (IOException e) {
                    LOGGER.debug(String.format( "Error while closing common words file '%s': %s",
                                    COMMON_WORDS_FILENAME,
                                    e));
                }
            }
        } else {
            result = new org.apache.lucene.analysis.standard.StandardAnalyzer(Version.LUCENE_CURRENT);
        }
        
        return result;
    }
    

}
