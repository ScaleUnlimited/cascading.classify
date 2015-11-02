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

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.util.Version;

@SuppressWarnings("serial")
public class WhitespaceAnalyzer extends LuceneAnalyzer {

    @Override
    public Analyzer createAnalyzer() {
        return new org.apache.lucene.analysis.core.WhitespaceAnalyzer(Version.LUCENE_CURRENT);
    }

}
