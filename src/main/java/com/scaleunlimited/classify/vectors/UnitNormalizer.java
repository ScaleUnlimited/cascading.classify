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

/**
 * Normalize our vector of counts into a vector of length 1 (unit vector)
 *
 */
@SuppressWarnings("serial")
public class UnitNormalizer extends BaseNormalizer {

    @Override
    public void normalize(Vector vector) {
        
        double length = Math.sqrt(vector.getLengthSquared());
        
        // Divide each vector coordinate by length, so we wind up with a unit vector.
        int cardinality = vector.size();
        for (int j = 0; j < cardinality; j++) {
            double curValue = vector.getQuick(j);
            if (curValue > 0.0) {
                vector.setQuick(j, curValue/length);
            }
        }
    }
}
