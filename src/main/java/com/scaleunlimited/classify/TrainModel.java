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

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


import cascading.flow.FlowProcess;
import cascading.operation.BaseOperation;
import cascading.operation.Function;
import cascading.operation.FunctionCall;
import cascading.operation.OperationCall;
import cascading.tuple.TupleEntryCollector;

import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.classify.analyzer.IAnalyzer;
import com.scaleunlimited.classify.datum.ModelDatum;
import com.scaleunlimited.classify.datum.TermsDatum;

@SuppressWarnings({"serial", "rawtypes"})
public class TrainModel
    extends BaseOperation<NullContext>
    implements Function<NullContext> {
    
    private static final Logger LOGGER = LoggerFactory.getLogger(TrainModel.class);
   
    private IAnalyzer _analyzer;
    private BaseModel _model;
    private TupleEntryCollector _outputCollector;
    private transient LoggingFlowProcess _flowProcess;

    public TrainModel(IAnalyzer analyzer, BaseModel model) {
        super(ModelDatum.FIELDS);
        _analyzer = analyzer;
        _model = model;
    }

    @SuppressWarnings("unchecked")
	@Override
    public void prepare(FlowProcess flowProcess,
                        OperationCall<NullContext> operationCall) {
        super.prepare(flowProcess, operationCall);
        _flowProcess = new LoggingFlowProcess(flowProcess);
        _flowProcess.addReporter(new LoggingFlowReporter());
        _analyzer.reset();
        _model.reset();
    }

    @SuppressWarnings("unchecked")
	@Override
    public void operate(FlowProcess flowProcess,
                        FunctionCall<NullContext> functionCall) {
        TermsDatum termsDatum =
            new TermsDatum(functionCall.getArguments().getTuple());
        if (_outputCollector == null) {
            _outputCollector = functionCall.getOutputCollector();
        }
        _model.addTrainingTerms(termsDatum);
        _flowProcess.increment(ClassifyCounters.TRAINING_TERMS_DATUM, 1);
    }

    @Override
    public void cleanup(FlowProcess flowProcess,
                        OperationCall<NullContext> operationCall) {
        _model.train();
        _analyzer.reset();
        try {
            ModelDatum modelDatum = new ModelDatum(_analyzer, _model);
            _outputCollector.add(modelDatum.getTuple());
        } catch (IOException e) {
            LOGGER.error("Unable to serialize model", e);
        }
        _flowProcess.dumpCounters();
        super.cleanup(flowProcess, operationCall);
    }
}