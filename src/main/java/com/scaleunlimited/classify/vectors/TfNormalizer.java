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

import java.util.List;

import org.apache.mahout.math.Vector;
import org.apache.mahout.math.function.DoubleDoubleFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Given a vector of counts, turn these into term frequency values.
 *
 */
@SuppressWarnings("serial")
public class TfNormalizer extends BaseNormalizer {
    private static final Logger LOGGER = LoggerFactory.getLogger(TfNormalizer.class);

    @Override
    public void normalize(List<? extends Vector> vectors) {
        if (vectors.size() == 0) {
            throw new IllegalArgumentException("Can't normalize empty list of vectors");
        }
        
        final int numDocs = vectors.size();
        LOGGER.debug(String.format( "Beginning normalization of %d vectors",
                                    numDocs));
        
        // Now, for each vector we want to normalize by total count.
        for (int i = 0; i < numDocs; i++) {
            Vector v = vectors.get(i);
            normalize(v);
        }

        LOGGER.debug(String.format( "Finished normalization of %d vectors",
                                    numDocs));
    }
    
    @Override
    public void normalize(Vector vector) {
        double totalCount = vector.zSum();
        vector.assign(new DoubleDoubleFunction() {
            
            @Override
            public double apply(double curValue, double totalCount) {
                return curValue / totalCount;
            }
        }, totalCount);
    }
}
