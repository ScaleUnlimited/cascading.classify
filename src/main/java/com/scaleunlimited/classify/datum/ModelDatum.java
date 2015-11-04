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
package com.scaleunlimited.classify.datum;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.DataInputBuffer;
import org.apache.hadoop.io.DataOutputBuffer;

import cascading.tuple.Fields;
import cascading.tuple.Tuple;

import com.scaleunlimited.cascading.BaseDatum;
import com.scaleunlimited.classify.BaseModel;
import com.scaleunlimited.classify.analyzer.IAnalyzer;
import com.scaleunlimited.classify.analyzer.TextDatumAnalyzer;

/**
 * Convenience datum for holding a classification model ({@link BaseModel} subclass)
 * as well as the analyzer ({@link TextDatumAnalyzer} subclass) that should be used to
 * pre-process (i.e., create {@link TermsDatum} documents for) the {@link TextDatum}
 * documents to be classified.
 */
@SuppressWarnings({"serial", "rawtypes"})
public class ModelDatum extends BaseDatum {

    public static final String ANALYZER_FN = fieldName(ModelDatum.class, "analyzer");
    public static final String MODEL_FN = fieldName(ModelDatum.class, "model");
    public static final String MODEL_DATA_FN = fieldName(ModelDatum.class, "modeldata");

    public static final Fields FIELDS = new Fields( ANALYZER_FN,
                                                    MODEL_FN,
                                                    MODEL_DATA_FN);
    
    public ModelDatum(Fields fields) {
        super(fields);
    }

    public ModelDatum(Tuple tuple) {
        super(FIELDS, tuple);
    }
    
    public ModelDatum(Fields fields, Tuple tuple) {
        super(fields, tuple);
    }

	public ModelDatum(IAnalyzer analyzer, BaseModel model) throws IOException {
        super(FIELDS);
        setAnalyzer(analyzer);
        setModel(model);
    }

    public IAnalyzer getAnalyzer() throws Exception {
        String className = _tupleEntry.getString(ANALYZER_FN);
        return (IAnalyzer)Class.forName(className).newInstance();
    }

    public void setAnalyzer(IAnalyzer analyzer) {
        _tupleEntry.setString(ANALYZER_FN, analyzer.getClass().getName());
    }
    
    public BaseModel getModel() throws Exception {
        String className = _tupleEntry.getString(MODEL_FN);
        BytesWritable modelData = (BytesWritable)(_tupleEntry.getObject(MODEL_DATA_FN));
        DataInputBuffer dib = new DataInputBuffer();
        dib.reset(modelData.getBytes(), modelData.getLength());
        BaseModel model = (BaseModel)Class.forName(className).newInstance();
        model.readFields(dib);
        return model;
    }

    public void setModel(BaseModel model) throws IOException {
        _tupleEntry.setString(MODEL_FN, model.getClass().getName());
        DataOutputBuffer dob = new DataOutputBuffer();
        model.write(dob);
        BytesWritable modelData = new BytesWritable(dob.getData());
        _tupleEntry.setObject(MODEL_DATA_FN, modelData);
    }
    
}
