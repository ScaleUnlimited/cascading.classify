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
package com.scaleunlimited.classify.pu;

import java.util.HashMap;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.TupleLogger;
import com.scaleunlimited.classify.AnalyzeTuple;
import com.scaleunlimited.classify.TrainModelPipe;
import com.scaleunlimited.classify.analyzer.IAnalyzer;
import com.scaleunlimited.classify.datum.TermsDatum;
import com.scaleunlimited.classify.datum.TextDatum;

@SuppressWarnings({"serial", "rawtypes"})
public class AnalyzeTrainingDataPipe extends SubAssembly {
    private static final Logger LOGGER = LoggerFactory.getLogger(AnalyzeTrainingDataPipe.class);
    
    private static final String POSITIVE_TERMS_PIPE_NAME =
        "positive terms";
    private static final String UNLABELED_TERMS_PIPE_NAME =
        "unlabeled terms";

    public AnalyzeTrainingDataPipe( Pipe positivePipe,
                                    Pipe unlabeledPipe,
                                    IAnalyzer analyzer) {
        super(positivePipe, unlabeledPipe);

        Pipe positiveTermsPipe =
            new Pipe(POSITIVE_TERMS_PIPE_NAME, positivePipe);
        positiveTermsPipe =
            new Each(positiveTermsPipe, new AnalyzeTuple(analyzer));
        positiveTermsPipe = TupleLogger.makePipe(positiveTermsPipe, true);
        
        Pipe unlabeledTermsPipe =
            new Pipe(UNLABELED_TERMS_PIPE_NAME, unlabeledPipe);
        unlabeledTermsPipe =
            new Each(unlabeledTermsPipe, new AnalyzeTuple(analyzer));
        unlabeledTermsPipe = TupleLogger.makePipe(unlabeledTermsPipe, true);
        
        setTails(positiveTermsPipe, unlabeledTermsPipe);
    }

    public Pipe getPositiveTermsTailPipe() {
        return getTailPipe(POSITIVE_TERMS_PIPE_NAME);
    }
    
    public Pipe getUnlabeledTermsTailPipe() {
        return getTailPipe(UNLABELED_TERMS_PIPE_NAME);
    }
    
    public Pipe getTailPipe(String pipeName) {
        for (Pipe tailPipe : getTails()) {
            if (tailPipe.getName().equals(pipeName)) {
                return tailPipe;
            }
        }
        return null;
    }
    
	public static Flow createTextFlow(BasePlatform platform, AnalyzeTrainingDataOptions options)
        throws Exception {
        
        return createFlow(platform, options, TextDatum.FIELDS);
    }

    public static Flow createFlow(  BasePlatform platform, AnalyzeTrainingDataOptions options,
                                    Fields inputFields)
        throws Exception {
        
        // Find working directory
        BasePath workingDirPath = platform.makePath(options.getWorkingDir());
        workingDirPath.assertExists("Working directory");
        
        // Try to instantiate the analyzer
        IAnalyzer analyzer =
            TrainModelPipe.makeAnalyzer(options.getAnalyzerName());

        // Set up the input sources
        BasePath positivePath = platform.makePath(workingDirPath, ClassifyPUConfig.POSITIVE_SUBDIR_NAME);
        positivePath.assertExists("Positive training directory");
        Tap positiveSource = platform.makeTap(platform.makeBinaryScheme(inputFields), positivePath);
        
        BasePath unlabeledPath = platform.makePath(workingDirPath, ClassifyPUConfig.UNLABELED_SUBDIR_NAME);
        unlabeledPath.assertExists("Unlabeled training directory");

        Tap unlabeledSource = platform.makeTap(platform.makeBinaryScheme(inputFields), unlabeledPath);
        
        // Clean out the reliably negative terms directories if any exist,
        // since we'll be iterating ExtractRNTermsWorkflow to refine them.
        // This forces ExtractRNTermsWorkflow to use UNLABELED_TERMS_SUBDIR_NAME
        // for input the first time.
        BasePath reliablyNegativePath = platform.makePath(workingDirPath, ClassifyPUConfig.RELIABLY_NEGATIVE_TERMS_SUBDIR_NAME);
        if (reliablyNegativePath.exists()) {
            LOGGER.info(String.format(  "Cleaning out existing reliably negative training terms in %s",
                                        reliablyNegativePath));
            reliablyNegativePath.delete(true);
        }
        for (int i = 0; i < ExtractRNTermsWorkflow.MAX_ITERATIONS; i++) {
            BasePath previousReliablyNegativePath =
                ExtractRNTermsWorkflow.makePreviousReliablyNegativePath(platform, reliablyNegativePath, i);
            if (previousReliablyNegativePath.exists()) {
                LOGGER.info(String.format(  "Cleaning out previous reliably negative training terms in %s",
                                            previousReliablyNegativePath));
                previousReliablyNegativePath.delete(true);
            }
        }
                
        // Analyze the input text into terms
        Pipe positivePipe = new Pipe("positive pipe");
        Pipe unlabeledPipe = new Pipe("unlabeled pipe");
        AnalyzeTrainingDataPipe analyzerPipe =
            new AnalyzeTrainingDataPipe(positivePipe, unlabeledPipe, analyzer);
        
        // Set up the output sinks
        BasePath unlabeledTermsPath = platform.makePath(workingDirPath, ClassifyPUConfig.UNLABELED_TERMS_SUBDIR_NAME);
        Tap unlabledTermsSink = platform.makeTap(platform.makeBinaryScheme(TermsDatum.FIELDS), 
                                        unlabeledTermsPath,
                                        SinkMode.REPLACE);
        BasePath positiveTermsPath = platform.makePath(workingDirPath, ClassifyPUConfig.POSITIVE_TERMS_SUBDIR_NAME);
        Tap positiveTermsSink = platform.makeTap(platform.makeBinaryScheme(TermsDatum.FIELDS),
                                        positiveTermsPath,
                                        SinkMode.REPLACE);
        
        // Build and return the workflow

        Map<String, Tap> sources = new HashMap<String, Tap>();
        sources.put(unlabeledPipe.getName(), unlabeledSource);
        sources.put(positivePipe.getName(), positiveSource);
        Map<String, Tap> sinks = new HashMap<String, Tap>();
        sinks.put(  analyzerPipe.getPositiveTermsTailPipe().getName(),
                    positiveTermsSink);
        sinks.put(  analyzerPipe.getUnlabeledTermsTailPipe().getName(),
                    unlabledTermsSink);
        
        FlowConnector flowConnector = platform.makeFlowConnector();

        return flowConnector.connect(   sources,
                                        sinks,
                                        analyzerPipe.getTails());
    }
}
