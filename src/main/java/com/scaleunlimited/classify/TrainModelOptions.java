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
package com.scaleunlimited.classify;

import org.kohsuke.args4j.Option;

public class TrainModelOptions extends ClassifyOptions {

    private static final String DEFAULT_ANALYZER_NAME = "Standard";
    private static final String DEFAULT_MODEL_NAME = "RawFeaturesLibLinear";

    private String _analyzerName = DEFAULT_ANALYZER_NAME;
    private String _modelName = DEFAULT_MODEL_NAME;

    @Option(name = "-analyzer", usage = "analyzer to use (class will be XXXAnalyzer)", required = false)
    public void setAnalyzerName(String analyzerName) {
        _analyzerName = analyzerName;
    }

    @Option(name = "-model", usage = "model to use (class will be XXXModel)", required = false)
    public void setModelName(String modelName) {
        _modelName = modelName;
    }

    public String getAnalyzerName() {
        return _analyzerName;
    }

    public String getModelName() {
        return _modelName;
    }
}
