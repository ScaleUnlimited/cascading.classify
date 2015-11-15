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

import java.util.Iterator;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.TupleLogger;
import com.scaleunlimited.classify.analyzer.IAnalyzer;
import com.scaleunlimited.classify.datum.DocDatum;
import com.scaleunlimited.classify.datum.ModelDatum;
import com.scaleunlimited.classify.datum.TermsDatum;
import com.scaleunlimited.classify.datum.TextDatum;

/**
 * Classifies each input {@link Tuple} using a classification model
 * ({@link BaseModel} subclass), and outputs a {@link DocDatum} representing
 * this classification (label and confidence score). An analyzer
 * (implementing {@link IAnalyzer}) is embedded within the model and is
 * responsible for extracting the terms from the input {@link Tuple}.
 * Most analyzers also payload the input {@link Tuple} (or its payload) in the
 * {@link TermsDatum} so that it can be payloaded in the output {@link DocDatum}.
 * 
 * @see {@link TrainModelPipe}, which builds such models from pre-labeled
 * training {@link Tuple} documents.
 * 
 */
@SuppressWarnings({"serial", "rawtypes"})
public class ClassifyDocsPipe extends SubAssembly {

    public ClassifyDocsPipe(Pipe inputPipe, IAnalyzer analyzer, BaseModel model) {
        super(inputPipe);
        
        // Analyze the input text into terms
        Pipe termsPipe = new Pipe("input terms", inputPipe);
        termsPipe = new Each(termsPipe, new AnalyzeTuple(analyzer));
        termsPipe = TupleLogger.makePipe(termsPipe, true);
        
        Pipe outputPipe = new Pipe("output docs", termsPipe);
        outputPipe = new Each(outputPipe, new ClassifyTerms(model));
        outputPipe = TupleLogger.makePipe(outputPipe, true);
        setTails(outputPipe);
    }
    
    public Pipe getOutputPipe() {
        return getTails()[0];
    }

    public static Flow createTextFlow(BasePlatform platform, ClassifyOptions options)
        throws Exception {
        
        return createFlow(platform, options, TextDatum.FIELDS);
    }

    /**
     * Return a workflow that uses a classification model read from a sequence
     * file to classify unlabeled {@link Tuple} documents read from another
     * sequence file, and then outputs a {@link DocDatum} representing the
     * classification of each.
     * 
     * @param platform      The cascading platform to use for running the flow
     * @param options       {@link ClassifyOptions#getWorkingDir()} contains
     * both {@link ClassifyConfig#UNCLASSIFIED_SUBDIR_NAME} and
     * {@link ClassifyConfig#MODEL_SUBDIR_NAME}, and is where
     * {@link ClassifyConfig#CLASSIFIED_SUBDIR_NAME} will be created.
     * @param inputFields   {@link Fields} in each input {@link Tuple}
     * @return              call its {@link Flow#complete()} method to execute
     * the workflow
     * @throws              Exception
     */
    public static Flow createFlow(  BasePlatform platform, ClassifyOptions options,
                                    Fields inputFields)
        throws Exception {
        
        // Find working directory
        BasePath workingDirPath = platform.makePath(options.getWorkingDir());
        workingDirPath.assertExists("Working directory");

        // Read in the model (including analyzer)
        BasePath modelPath = platform.makePath(workingDirPath, ClassifyConfig.MODEL_SUBDIR_NAME);
        ModelDatum modelDatum = readModel(platform, modelPath);
        
        // Set up the input source
        BasePath inputPath = platform.makePath(workingDirPath, ClassifyConfig.UNCLASSIFIED_SUBDIR_NAME);
        inputPath.assertExists("Input directory");
        Tap inputSource = platform.makeTap(platform.makeBinaryScheme(inputFields), inputPath);
        
        // Analyze the input text into terms and then classify the result
        Pipe inputPipe = new Pipe("input pipe");
        ClassifyDocsPipe classifierPipe =
            new ClassifyDocsPipe(   inputPipe,
                                    modelDatum.getAnalyzer(),
                                    modelDatum.getModel());

        // Set up the output sink
        BasePath outputPath = platform.makePath(workingDirPath, ClassifyConfig.CLASSIFIED_SUBDIR_NAME);
        Tap outputSink = platform.makeTap(   platform.makeBinaryScheme(DocDatum.FIELDS),
                                    outputPath,
                                    SinkMode.REPLACE);
        
        // Build and return the workflow
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(   inputSource,
                                        outputSink,
                                        classifierPipe.getOutputPipe());
    }

    @SuppressWarnings("unchecked")
	public static ModelDatum readModel( BasePlatform platform,
                                        BasePath modelPath)
        throws Exception {
        
        modelPath.assertExists("Model directory");
        Tap modelSource = platform.makeTap(platform.makeBinaryScheme(ModelDatum.FIELDS), modelPath);
        Iterator<TupleEntry> iter = modelSource.openForRead(platform.makeFlowProcess());
        if (!(iter.hasNext())) {
            throw new IllegalStateException(String.format(  "Model directory %s doesn't contain any classifiers",
                                                            modelPath));
        }
        ModelDatum result = new ModelDatum(iter.next().getTuple());
        if (iter.hasNext()) {
            throw new IllegalStateException(String.format(  "Model directory %s has more than one classifier",
                                                            modelPath));
        }
        return result;
    }
}
