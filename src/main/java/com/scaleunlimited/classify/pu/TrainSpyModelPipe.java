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
import java.util.Random;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntry;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.BaseSplitter;
import com.scaleunlimited.cascading.SplitterAssembly;
import com.scaleunlimited.cascading.TupleLogger;
import com.scaleunlimited.classify.BaseModel;
import com.scaleunlimited.classify.TrainLogisticModelPipe;
import com.scaleunlimited.classify.TrainModelPipe;
import com.scaleunlimited.classify.analyzer.NullAnalyzer;
import com.scaleunlimited.classify.datum.ModelDatum;
import com.scaleunlimited.classify.datum.TermsDatum;

@SuppressWarnings({"serial", "rawtypes"})
public class TrainSpyModelPipe extends SubAssembly {
    
    private static final double SPY_FRACTION = 0.15;
    private static final String MODEL_PIPE_NAME = "spy model pipe";
    private static final String SPIES_PIPE_NAME = "spies pipe";

    private static class SpySplitter extends BaseSplitter {

        private Random _random;
        private long _randomSeed;
        
        SpySplitter(long randomSeed) {
            _randomSeed  = randomSeed;
        }

        @Override
        public String getLHSName() {
            return "spy docs";
        }

        @Override
        public boolean isLHS(TupleEntry tupleEntry) {
            if (_random == null) {
                _random = new Random(_randomSeed);
            }
            return(_random.nextDouble() < SPY_FRACTION);
        }
    }

    public TrainSpyModelPipe(   Pipe positivePipe,
                                Pipe unlabeledPipe,
                                BaseModel model) {
        this(   positivePipe,
                unlabeledPipe,
                model,
                System.currentTimeMillis());
    }
    
    // Grab a random sample S of P, then train a model based on P-S and U+S.
    public TrainSpyModelPipe(   Pipe positivePipe,
                                Pipe unlabeledPipe,
                                BaseModel model,
                                long randomSeed) {
        super();
        
        // Choose random sample S of the positive docs P
        SplitterAssembly splitter =
            new SplitterAssembly(   positivePipe,
                                    new SpySplitter(randomSeed),
                                    ClassifyPUCounters.POSITIVE_SPY,
                                    ClassifyPUCounters.POSITIVE_NON_SPY);
        Pipe spiesPipe = new Pipe(SPIES_PIPE_NAME, splitter.getLHSPipe());
        spiesPipe = TupleLogger.makePipe(spiesPipe, true);
        
        // Train a model M based on P-S and U+S
        Pipe remainingPositivePipe = new Pipe(  "remaining positive",
                                                splitter.getRHSPipe());
        Pipe unlabeledAndSpiesPipe =
            new GroupBy(Pipe.pipes(unlabeledPipe, spiesPipe));
        TrainLogisticModelPipe trainModelPipe =
            new TrainLogisticModelPipe( remainingPositivePipe,
                                        unlabeledAndSpiesPipe,
                                        new NullAnalyzer(),
                                        model,
                                        MODEL_PIPE_NAME);
        setTails(trainModelPipe.getModelTailPipe(), spiesPipe);
    }
    
    public Pipe getModelTailPipe() {
        return getTailPipe(MODEL_PIPE_NAME);
    }
    
    public Pipe getSpiesTailPipe() {
        return getTailPipe(SPIES_PIPE_NAME);
    }
    
    public Pipe getTailPipe(String pipeName) {
        for (Pipe tailPipe : getTails()) {
            if (tailPipe.getName().equals(pipeName)) {
                return tailPipe;
            }
        }
        return null;
    }
    
    public static Flow createFlow(BasePlatform platform, TrainSpyModelOptions options)
        throws Exception {
        
        // Find working directory
        BasePath workingDirPath = platform.makePath(options.getWorkingDir());
        workingDirPath.assertExists("Working directory");
        
        // Try to instantiate the modeler
        BaseModel model = TrainModelPipe.makeModel(options.getModelName());

        // Set up the input sources
        BasePath unlabeledPath = platform.makePath(workingDirPath, ClassifyPUConfig.UNLABELED_TERMS_SUBDIR_NAME);
        unlabeledPath.assertExists("Unlabeled training terms directory");
        Tap unlabeledSource = platform.makeTap(platform.makeBinaryScheme(TermsDatum.FIELDS), unlabeledPath);
        BasePath positivePath = platform.makePath(workingDirPath, ClassifyPUConfig.POSITIVE_TERMS_SUBDIR_NAME);
        positivePath.assertExists("Positive training terms directory");
        Tap positiveSource = platform.makeTap(platform.makeBinaryScheme(TermsDatum.FIELDS), positivePath);
        
        // Grab a random sample S of P, then train a model based on P-S and U+S.
        // Note that training data has been pre-analyzed.
        Pipe unlabeledPipe = new Pipe("unlabeled pipe");
        Pipe positivePipe = new Pipe("positive pipe");
        TrainSpyModelPipe trainModelPipe =
            new TrainSpyModelPipe(  positivePipe,
                                    unlabeledPipe,
                                    model,
                                    options.getRandomSeed());

        // Set up the output sinks
        BasePath modelPath = platform.makePath(workingDirPath, ClassifyPUConfig.SPY_MODEL_SUBDIR_NAME);
        Tap modelSink = platform.makeTap(platform.makeBinaryScheme(ModelDatum.FIELDS),
                                modelPath,
                                SinkMode.REPLACE);
        BasePath spiesPath = platform.makePath(workingDirPath, ClassifyPUConfig.SPIES_SUBDIR_NAME);
        Tap spiesSink = platform.makeTap(platform.makeBinaryScheme(TermsDatum.FIELDS),
                                spiesPath,
                                SinkMode.REPLACE);
        
        // Build and return the workflow
        Map<String, Tap> sources = new HashMap<String, Tap>();
        sources.put(unlabeledPipe.getName(), unlabeledSource);
        sources.put(positivePipe.getName(), positiveSource);
        Map<String, Tap> sinks = new HashMap<String, Tap>();
        sinks.put(trainModelPipe.getModelTailPipe().getName(), modelSink);
        sinks.put(trainModelPipe.getSpiesTailPipe().getName(), spiesSink);
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(   sources,
                                        sinks,
                                        trainModelPipe.getTails());
    }
}
