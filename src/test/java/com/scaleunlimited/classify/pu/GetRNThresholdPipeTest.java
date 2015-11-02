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

import java.io.IOException;

import org.junit.Before;
import org.junit.Test;

import cascading.flow.Flow;

import com.scaleunlimited.classify.ClassifyOptions;

public class GetRNThresholdPipeTest extends TrainSpyModelPipeTest {

    @Before
    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (!super.sinkDirsExist()) {
            TrainSpyModelPipeTest superTest =
                new TrainSpyModelPipeTest();
            superTest.setUp();
            superTest.createFlow();
        }
    }

    @Test
    @Override
    public void createFlow() throws Exception {
        ClassifyOptions options = new ClassifyOptions();
        options.setDebugLogging(false);
        options.setWorkingDir(WORKING_DIR);
        Flow flow = GetRNThresholdPipe.createFlow(_platform, options);
        flow.complete();
        this.checkSinkDirsExist();
        // TODO CSc Check that 15% of spies score below threshold?
    }

    @Override
    public boolean sinkDirsExist() throws IOException {
        return(_thresholdPath.exists());
    }
    
    @Override
    public void checkSinkDirsExist() throws IOException {
        _thresholdPath.assertExists("Reliably negative threshold directory");
    }

}
