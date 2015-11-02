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

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cascading.flow.Flow;
import cascading.flow.FlowConnector;
import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;
import cascading.tap.SinkMode;
import cascading.tap.Tap;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.BasePath;
import com.scaleunlimited.cascading.BasePlatform;
import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.cascading.TupleLogger;
import com.scaleunlimited.classify.BaseModel;
import com.scaleunlimited.classify.ClassifyDocsPipe;
import com.scaleunlimited.classify.ClassifyOptions;
import com.scaleunlimited.classify.TrainLogisticModelPipe;
import com.scaleunlimited.classify.analyzer.IAnalyzer;
import com.scaleunlimited.classify.datum.DocDatum;
import com.scaleunlimited.classify.datum.ModelDatum;
import com.scaleunlimited.classify.datum.TermsDatum;
import com.scaleunlimited.classify.datum.ThresholdDatum;

//import edu.emory.mathcs.backport.java.util.Collections;

@SuppressWarnings({"serial", "rawtypes"})
public class GetRNThresholdPipe extends SubAssembly {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(GetRNThresholdPipe.class);

    // TODO CSc Make this an option?
    private static final double NOISE_LEVEL = 0.15f;

    private static class GetThreshold
        extends BaseOperation<NullContext>
        implements Function<NullContext> {

        double _noiseLevel;
        List<Float> _spyScores;
        private TupleEntryCollector _outputCollector;
        private transient LoggingFlowProcess _flowProcess;
        
        public GetThreshold(double noiseLevel) {
            super(ThresholdDatum.FIELDS);
            _noiseLevel = noiseLevel;
        }

        @SuppressWarnings("unchecked")
		@Override
        public void prepare(FlowProcess flowProcess,
                            OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            _flowProcess = new LoggingFlowProcess(flowProcess);
            _flowProcess.addReporter(new LoggingFlowReporter());
            _spyScores = new ArrayList<Float>();
        }

        @Override
        public void operate(FlowProcess flowProcess,
                            FunctionCall<NullContext> functionCall) {
            DocDatum docDatum =
                new DocDatum(functionCall.getArguments().getTuple());
            if (_outputCollector == null) {
                _outputCollector = functionCall.getOutputCollector();
            }
            _spyScores.add(TrainLogisticModelPipe.getPositiveScore(docDatum));
        }

        @Override
        public void cleanup(FlowProcess flowProcess,
                            OperationCall<NullContext> operationCall) {
            Collections.sort(_spyScores);
            int numSpiesBelowThreshold =
                (int)(((double)_spyScores.size()) * _noiseLevel);
            double threshold = _spyScores.get(numSpiesBelowThreshold);
            LOGGER.info("Reliably negative threshold: " + threshold);
            ThresholdDatum thresholdDatum = new ThresholdDatum(threshold);
            _outputCollector.add(thresholdDatum.getTuple());
            _flowProcess.increment( ClassifyPUCounters.NOISY_SPY_BELOW_THRESHOLD,
                                    numSpiesBelowThreshold);
            _flowProcess.dumpCounters();
            super.cleanup(flowProcess, operationCall);
        }
    }

    // Determine RN threshold t, based on M-Prob(S)
    // such that M-Prob(SN[i]) < t, where SN = 15% of S
    public GetRNThresholdPipe(  Pipe spiesPipe,
                                IAnalyzer analyzer,
                                BaseModel model,
                                double noiseLevel) {
        super();

        ClassifyDocsPipe classifyPipe =
            new ClassifyDocsPipe(spiesPipe, analyzer, model);
        Pipe classifiedSpiesPipe = classifyPipe.getOutputPipe();
        Pipe thresholdPipe = new Pipe("threshold", classifiedSpiesPipe);
        thresholdPipe = new Each(thresholdPipe, new GetThreshold(noiseLevel));
        thresholdPipe = TupleLogger.makePipe(thresholdPipe, true);
        setTails(thresholdPipe);
    }
    
    public Pipe getThresholdTailPipe() {
        return getTails()[0];
    }
    
    public static Flow createFlow(BasePlatform platform, ClassifyOptions options)
        throws Exception {
        
        // Find working directory
        BasePath workingDirPath = platform.makePath(options.getWorkingDir());
        workingDirPath.assertExists("Working directory");

        // Read in the model (including analyzer)
        BasePath modelPath = platform.makePath(workingDirPath, ClassifyPUConfig.SPY_MODEL_SUBDIR_NAME);
        ModelDatum modelDatum =
            ClassifyDocsPipe.readModel(platform, modelPath);
        
        // Set up the input source
        BasePath spiesPath = platform.makePath(workingDirPath, ClassifyPUConfig.SPIES_SUBDIR_NAME);
        spiesPath.assertExists("Positive spies training terms directory");
        Tap spiesSource = platform.makeTap( platform.makeBinaryScheme(TermsDatum.FIELDS), spiesPath);
        
        // Classify the spies to determine a positive scoring threshold that
        // woud make only the outlier spies negative (i.e., assume 15% are noise).
        // Note that although the spies are pre-analyzed, the spy model analyzer
        // is just NullAnalyzer.
        Pipe spiesPipe = new Pipe("spies pipe");
        GetRNThresholdPipe thresholdPipe =
            new GetRNThresholdPipe( spiesPipe,
                                    modelDatum.getAnalyzer(),
                                    modelDatum.getModel(),
                                    NOISE_LEVEL);

        // Set up the output sink
        BasePath thresholdPath = platform.makePath(   workingDirPath,
                        ClassifyPUConfig.RELIABLY_NEGATIVE_THRESHOLD_SUBDIR_NAME);
        Tap thresholdSink = platform.makeTap(platform.makeBinaryScheme(ThresholdDatum.FIELDS),
                                    thresholdPath,
                                    SinkMode.REPLACE);
        
        FlowConnector flowConnector = platform.makeFlowConnector();
        return flowConnector.connect(   spiesSource,
                                        thresholdSink,
                                        thresholdPipe.getThresholdTailPipe());
    }
}
