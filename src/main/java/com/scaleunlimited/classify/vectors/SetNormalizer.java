/**
 * Copyright (c) 2015 Scale Unlimited, Inc.
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

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.DoubleDoubleFunction;

/**
 * Normalize our vector as if every count was the same.
 *
 */
@SuppressWarnings("serial")
public class SetNormalizer extends BaseNormalizer {

    public void normalize(Vector vector) {
        // First count the number of non-zero values.
        double valueCount = vector.getNumNonZeroElements();
        
        // Set each non-zero value to 1/count of non-zero values, so that
        // it's as if these all have a count of 1, so they have equal TF.
        vector.assign(new DoubleDoubleFunction() {
            
            @Override
            public double apply(double curValue, double normalizedValue) {
                return (curValue > 0.0 ? normalizedValue : 0);
            }
            
        }, 1.0/valueCount);
    }
}
