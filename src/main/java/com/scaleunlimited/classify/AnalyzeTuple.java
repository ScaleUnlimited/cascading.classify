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

import com.scaleunlimited.cascading.LoggingFlowProcess;
import com.scaleunlimited.cascading.LoggingFlowReporter;
import com.scaleunlimited.cascading.NullContext;
import com.scaleunlimited.classify.analyzer.IAnalyzer;
import com.scaleunlimited.classify.datum.TermsDatum;

@SuppressWarnings({"serial", "rawtypes"})
public class AnalyzeTuple extends BaseOperation<NullContext> implements Function<NullContext> {
    
    private IAnalyzer _analyzer;
	private transient LoggingFlowProcess _flowProcess;

    public AnalyzeTuple(IAnalyzer analyzer) {
        super(TermsDatum.FIELDS);
        _analyzer = analyzer;
    }

    @SuppressWarnings("unchecked")
	@Override
    public void prepare(FlowProcess flowProcess, OperationCall<NullContext> operationCall) {
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
        if (termsDatum == null) {
            _flowProcess.increment(ClassifyCounters.SKIPPED_INPUT_TUPLE, 1);
        } else {
            functionCall.getOutputCollector().add(termsDatum.getTuple());
            _flowProcess.increment(ClassifyCounters.ANALYZED_INPUT_TUPLE, 1);
        }
    }

    @Override
    public void cleanup(FlowProcess flowProcess,
                        OperationCall<NullContext> operationCall) {
        _flowProcess.dumpCounters();
        super.cleanup(flowProcess, operationCall);
    }
}