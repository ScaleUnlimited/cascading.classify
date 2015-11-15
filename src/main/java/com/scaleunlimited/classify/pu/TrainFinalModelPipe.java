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

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.SinkMode;
import cascading.tap.Tap;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.TupleLogger;
import com.scaleunlimited.classify.BaseModel;
import com.scaleunlimited.classify.TrainLogisticModelPipe;
import com.scaleunlimited.classify.TrainModel;
import com.scaleunlimited.classify.TrainModelOptions;
import com.scaleunlimited.classify.TrainModelPipe;
import com.scaleunlimited.classify.analyzer.IAnalyzer;
import com.scaleunlimited.classify.analyzer.NullAnalyzer;
import com.scaleunlimited.classify.datum.ModelDatum;
import com.scaleunlimited.classify.datum.TermsDatum;

@SuppressWarnings({"serial", "rawtypes"})
public class TrainFinalModelPipe extends SubAssembly {

    public TrainFinalModelPipe( Pipe positivePipe,
                                Pipe reliablyNegativePipe,
                                IAnalyzer analyzer,
                                BaseModel model) {
        super(positivePipe);
        
        // Label the positive/negative training terms
        // Note that we've already analyzed this training data
        IAnalyzer nullAnalyzer = new NullAnalyzer();
        positivePipe =
            new Each(   positivePipe,
                        new TrainLogisticModelPipe.GetAndLabelTrainingTerms(nullAnalyzer,
                                                                            true));
        positivePipe = TupleLogger.makePipe(positivePipe, true);
        
        reliablyNegativePipe =
            new Each(   reliablyNegativePipe,
                        new TrainLogisticModelPipe.GetAndLabelTrainingTerms(nullAnalyzer,
                                                                            false));
        reliablyNegativePipe = TupleLogger.makePipe(reliablyNegativePipe, true);
        
        // Train a new model using those terms
        Pipe[] trainingPipes = Pipe.pipes(positivePipe, reliablyNegativePipe);
        Pipe trainingPipe = new Pipe("training terms", new GroupBy(trainingPipes));
        Pipe modelTailPipe = new Pipe("model", trainingPipe);
        modelTailPipe = new Each(modelTailPipe, new TrainModel(analyzer, model));
        setTails(modelTailPipe);
    }
    
    public Pipe getModelTailPipe() {
        return getTails()[0];
    }

    public static Flow createFlow(BasePlatform platform, TrainModelOptions options)
        throws Exception {
    
        // Find working directory
        BasePath workingDirPath = platform.makePath(options.getWorkingDir());
        workingDirPath.assertExists("Working directory");
    
        // Try to instantiate the analyzer and modeler
        IAnalyzer analyzer = TrainModelPipe.makeAnalyzer(options.getAnalyzerName());
        BaseModel model = TrainModelPipe.makeModel(options.getModelName());
    
        // Set up the input sources
        BasePath positivePath = platform.makePath(workingDirPath, ClassifyPUConfig.POSITIVE_TERMS_SUBDIR_NAME);
        positivePath.assertExists("Positive training terms directory");
        Tap positiveSource = platform.makeTap(platform.makeBinaryScheme(TermsDatum.FIELDS), positivePath);
        
        BasePath reliablyNegativePath = platform.makePath(workingDirPath, ClassifyPUConfig.RELIABLY_NEGATIVE_TERMS_SUBDIR_NAME);
        reliablyNegativePath.assertExists("Reliably negative training terms directory");
        Tap reliablyNegativeSource = platform.makeTap(platform.makeBinaryScheme(TermsDatum.FIELDS),
                                                reliablyNegativePath);
        
        // Label the training terms and train the model from them
        // Note that we've already analyzed the input data
        Pipe positivePipe = new Pipe("positive pipe");
        Pipe reliablyNegativePipe = new Pipe("reliably negative pipe");
        TrainFinalModelPipe trainerPipe =
            new TrainFinalModelPipe(positivePipe,
                                    reliablyNegativePipe,
                                    analyzer,
                                    model);
    
        // Set up the output sink
        BasePath modelPath = platform.makePath(workingDirPath, ClassifyPUConfig.MODEL_SUBDIR_NAME);
        Tap modelSink = platform.makeTap(platform.makeBinaryScheme(ModelDatum.FIELDS),
                                modelPath,
                                SinkMode.REPLACE);
        
        // Build and return the workflow
        Map<String, Tap> sources = new HashMap<String, Tap>();
        sources.put(reliablyNegativePipe.getName(), reliablyNegativeSource);
        sources.put(positivePipe.getName(), positiveSource);
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(   sources,
                                        modelSink,
                                        trainerPipe.getModelTailPipe());
    }
}
