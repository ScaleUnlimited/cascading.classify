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

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.TupleLogger;
import com.scaleunlimited.classify.analyzer.IAnalyzer;
import com.scaleunlimited.classify.datum.ModelDatum;
import com.scaleunlimited.classify.datum.TextDatum;

/**
 * Trains a new model ({@link BaseModel} subclass) using pre-labeled
 * {@link Tuple} training documents (after using an {@link IAnalyzer} to extract
 * the terms and label from each). Afterward, the model (output as a single
 * {@link ModelDatum}) can be used to classify an unlabeled {@link Tuple}
 * (e.g., by {@link ClassifyDocsPipe}).
 * 
 */
@SuppressWarnings({"serial", "rawtypes"})
public class TrainModelPipe extends SubAssembly {
    
    public TrainModelPipe(  Pipe trainingPipe,
                            IAnalyzer analyzer,
                            BaseModel model) {
        this(trainingPipe, analyzer, model, "model");
    }
    
    public TrainModelPipe(  Pipe trainingPipe,
                            IAnalyzer analyzer,
                            BaseModel model,
                            String modelTailPipeName) {
        super();
        
        // Analyze the training text into terms
        Pipe termsPipe = new Pipe("training terms", trainingPipe);
        termsPipe = new Each(termsPipe, new AnalyzeTuple(analyzer));
        termsPipe = TupleLogger.makePipe(termsPipe, true);
        
        // Train a new model using those terms
        Pipe modelPipe = new Pipe(modelTailPipeName, termsPipe);
        modelPipe = new Each(modelPipe, new TrainModel(analyzer, model));
        
        setTails(modelPipe);
    }
    
    public Pipe getModelTailPipe() {
        return getTails()[0];
    }

    public static Flow createTextFlow(BasePlatform platform, TrainModelOptions options)
        throws Exception {
        
        return createFlow(platform, options, TextDatum.FIELDS);
    }

    /**
     * Return a workflow that builds a new model based on pre-labeled
     * training {@link Tuple} documents read from a sequence file,
     * and then writes the result to another sequence file as a single
     * {@link ModelDatum}. Afterward, the model can be used to classify
     * an unlabeled {@link Tuple} (e.g., by {@link ClassifyDocsPipe#createFlow}).
     * 
     * @param platform      The cascading platform to use when creating the flow
     * @param options       {@link ClassifyOptions#getWorkingDir()} contains
     * {@link ClassifyConfig#TRAINING_SUBDIR_NAME} and is where
     * {@link ClassifyConfig#MODEL_SUBDIR_NAME} will be created.<br>
     * {@link TrainModelOptions#getAnalyzerName()} specifies the analyzer used
     * to extract the terms and label from each training {@link Tuple}
     * ("Analyzer" suffix is appended before calling Class.forName).<br>
     * {@link TrainModelOptions#getModelName()} specifies the type of model
     * to be built ("Model" suffix is appended before calling Class.forName).
     * @param inputFields   {@link Fields} in each training {@link Tuple}
     * @return              call its {@link Flow#complete()} method to execute
     * the workflow
     * @throws Exception 
     */
    public static Flow createFlow(  BasePlatform platform,
                                    TrainModelOptions options,
                                    Fields inputFields)
        throws Exception {
    
        BasePath workingDirPath = platform.makePath(options.getWorkingDir());
        workingDirPath.assertExists("Working directory");

        // Try to instantiate the analyzer and modeler
        IAnalyzer analyzer = makeAnalyzer(options.getAnalyzerName());
        BaseModel model = makeModel(options.getModelName());

        // Set up the input source
        BasePath trainingPath = platform.makePath(workingDirPath, ClassifyConfig.TRAINING_SUBDIR_NAME);
        trainingPath.assertExists("Training directory");
        Tap trainingSource = platform.makeTap(   platform.makeBinaryScheme(inputFields),
                                        trainingPath);
        
        // Analyze the training text into terms and train the model from them
        Pipe trainingPipe = new Pipe("training pipe");
        TrainModelPipe trainerPipe =
            new TrainModelPipe(trainingPipe, analyzer, model);

        // Set up the output sink
        BasePath modelPath = platform.makePath(workingDirPath, ClassifyConfig.MODEL_SUBDIR_NAME);
        Tap modelSink = platform.makeTap(platform.makeBinaryScheme(ModelDatum.FIELDS),
                                modelPath,
                                SinkMode.REPLACE);
        
        // Build and return the workflow
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(   trainingSource,
                                        modelSink,
                                        trainerPipe.getModelTailPipe());
    }

    public static BaseModel makeModel(String modelName) {
        BaseModel result;
        String fullModelName = "com.scaleunlimited.classify." + modelName + "Model";
        try {
            result = (BaseModel)Class.forName(fullModelName).newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Can't instantiate Model named " + fullModelName);
        }
        result.reset();
        return result;
    }

    public static IAnalyzer makeAnalyzer(String analyzerName) {
        IAnalyzer result;
        String fullAnalyzerName = "com.scaleunlimited.classify.analyzer." + analyzerName + "Analyzer";
        try {
            result = (IAnalyzer)Class.forName(fullAnalyzerName).newInstance();
        } catch (Exception e) {
            throw new IllegalArgumentException("Can't instantiate Analyzer named " + fullAnalyzerName);
        }
        result.reset();
        return result;
    }
}
