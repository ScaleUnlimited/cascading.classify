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
package com.scaleunlimited.classify.vectors;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.WritableComparable;
import org.apache.mahout.math.Vector;
import org.apache.mahout.math.VectorWritable;

public class WritableComparableVector implements WritableComparable<WritableComparableVector> {

	private static Configuration CONF = new Configuration();
	
    private Vector _vector;
    
    public WritableComparableVector(Vector vector) {
        _vector = vector;
    }
    
    public Vector getVector() {
        return _vector;
    }

    @Override
    public void readFields(DataInput in) throws IOException {
        VectorWritable v = new VectorWritable();
        
        // VectorWritable relies on having a valid conf - not sure how
        // that normally gets set up w/Mahout, but this seems to work.
        v.setConf(CONF);
        v.readFields(in);
        _vector = v.get();
    }

    @Override
    public void write(DataOutput out) throws IOException {
        new VectorWritable(_vector).write(out);
    }

    @Override
    public int compareTo(WritableComparableVector o) {
        // Never used as key, so we don't need to return anything special
        return 0;
    }
    
}
