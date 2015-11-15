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

import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.pipe.Each;
import cascading.pipe.GroupBy;
import cascading.pipe.Pipe;
import cascading.pipe.SubAssembly;

import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.cascading.TupleLogger;
import com.scaleunlimited.classify.analyzer.IAnalyzer;
import com.scaleunlimited.classify.datum.DocDatum;
import com.scaleunlimited.classify.datum.TermsDatum;

@SuppressWarnings({"serial", "rawtypes"})
public class TrainLogisticModelPipe extends SubAssembly {
    
    public static final String POSITIVE_LABEL = "POSITIVE";
    public static final String NEGATIVE_LABEL = "NEGATIVE";
    
    // TODO CSc Override AnalyzeTuple somehow?
    public static class GetAndLabelTrainingTerms
        extends BaseOperation<NullContext>
        implements Function<NullContext> {
        
        private IAnalyzer _analyzer;
        private boolean _positive;
        private transient LoggingFlowProcess _flowProcess;
    
        public GetAndLabelTrainingTerms(IAnalyzer analyzer,
                                        boolean positive) {
            super(TermsDatum.FIELDS);
            _analyzer = analyzer;
            _positive = positive;
        }
        
        @SuppressWarnings("unchecked")
		@Override
        public void prepare(FlowProcess flowProcess,
                            OperationCall<NullContext> operationCall) {
            super.prepare(flowProcess, operationCall);
            _flowProcess = new LoggingFlowProcess(flowProcess);
            _flowProcess.addReporter(new LoggingFlowReporter());
            _analyzer.reset();
        }
        
        @Override
        public void operate(FlowProcess flowProcess,
                            FunctionCall<NullContext> functionCall) {
            TermsDatum termsDatum =
                _analyzer.getTermsDatum(functionCall.getArguments().getTuple());
            if (termsDatum != null) {
                if (_positive) {
                    termsDatum.setLabel(POSITIVE_LABEL);
                    _flowProcess.increment(LogisticCounters.POSITIVE_TRAINING_TERMS_DATUM, 1);
                } else {
                    termsDatum.setLabel(NEGATIVE_LABEL);
                    _flowProcess.increment(LogisticCounters.NEGATIVE_TRAINING_TERMS_DATUM, 1);
                }
                functionCall.getOutputCollector().add(termsDatum.getTuple());
            }
        }

        @Override
        public void cleanup(FlowProcess flowProcess,
                            OperationCall<NullContext> operationCall) {
            _flowProcess.dumpCounters();
            super.cleanup(flowProcess, operationCall);
        }
    }    
    
    public static float getPositiveScore(DocDatum docDatum) {
        float positiveScore;
        if (docDatum.getLabel().equals(POSITIVE_LABEL)) {
            positiveScore = docDatum.getScore();
        } else {
            positiveScore = (1.0f - docDatum.getScore());
        }
        return positiveScore;
    }
    
    public TrainLogisticModelPipe(  Pipe positivePipe,
                                    Pipe negativePipe,
                                    IAnalyzer analyzer,
                                    BaseModel model) {
        this(positivePipe, negativePipe, analyzer, model, "model");
    }
    
    public TrainLogisticModelPipe(  Pipe positivePipe,
                                    Pipe negativePipe,
                                    IAnalyzer analyzer,
                                    BaseModel model,
                                    String modelTailPipeName) {
        super(positivePipe, negativePipe);
        
        // Analyze the training text into terms
        positivePipe =
            new Each(positivePipe, new GetAndLabelTrainingTerms(analyzer, true));
        positivePipe = TupleLogger.makePipe(positivePipe, true);
        
        negativePipe =
            new Each(negativePipe, new GetAndLabelTrainingTerms(analyzer, false));
        negativePipe = TupleLogger.makePipe(negativePipe, true);
        
        // Train a new model using those terms
        Pipe[] trainingPipes = Pipe.pipes(positivePipe, negativePipe);
        Pipe trainingPipe = new Pipe("training terms", new GroupBy(trainingPipes));
        Pipe modelTailPipe = new Pipe(modelTailPipeName, trainingPipe);
        modelTailPipe = new Each(modelTailPipe, new TrainModel(analyzer, model));
        setTails(modelTailPipe);
    }
    
    public Pipe getModelTailPipe() {
        return getTails()[0];
    }
}
