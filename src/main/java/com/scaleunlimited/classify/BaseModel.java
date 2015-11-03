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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.Writable;

import com.scaleunlimited.classify.datum.DocDatum;


/**
 * Classification model, which can be trained using a set of pre-labeled
 * T documents, and which can then be used to classify
 * a set of unlabeled T documents, outputting the
 * classification of each as a {@link DocDatum} document.
 */
@SuppressWarnings("serial")
public abstract class BaseModel<T> implements Writable, Serializable {
    
    public static final String NOT_YET_LABELED = "";
    public static final float NOT_YET_SCORED = Float.NaN;
    
    protected BaseModel() {
    }
    
    /**
     * @param termsDatum input document terms (with label) to help train model
     */
    abstract public void addTrainingTerms(T termsDatum);

    /**
     * Use all training documents added via {@link #addTrainingTerms(T)}
     * since the last call to {@link #train()} to train the classification model
     * (i.e., replacing out any model definition from the previous call).
     */
    abstract public void train();
    
    /**
     * @param datum (unlabeled) input document terms to be classified
     * @return classification of input document terms
     */
    abstract public DocDatum classify(T datum);
    
    /**
     * Generate details about the model.
     * 
     * @return Text description of the model.
     */
    abstract public String getDetails();
    
    /**
     * Initialize the newly constructed (presumably recently deserialized
     * or otherwise constructed) model, particularly its transient fields.
     */
    public void reset() {
        // Base does nothing.
    }
    
    public void stats() {
        // Base does nothing.
    }

    /**
     * @param out stream into which string list should be serialized
     * @param string list to be serialized
     * @throws IOException
     */
    protected static void writeStrings(DataOutput out, List<String> strings) throws IOException {
        out.writeInt(strings.size());
        for (String string : strings) {
            out.writeUTF(string);
        }
    }
    
    /**
     * @param in stream from which string list should be deserialized
     * @return deserialized string list
     * @throws IOException
     */
    protected static List<String> readStrings(DataInput in) throws IOException {
        int numStrings = in.readInt();
        List<String> result = new ArrayList<String>(numStrings);
        for (int i = 0; i < numStrings; i++) {
            result.add(in.readUTF());
        }
        
        return result;
    }

}
