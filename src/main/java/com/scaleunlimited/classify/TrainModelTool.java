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

import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

import cascading.flow.Flow;

import com.scaleunlimited.cascading.BaseTool;
import com.scaleunlimited.cascading.hadoop.HadoopPlatform;

public class TrainModelTool extends BaseTool {

    @SuppressWarnings("rawtypes")
	public static void main(String[] args) {
        TrainModelOptions options = new TrainModelOptions();
        CmdLineParser parser = parse(args, options);
        
        try {
            parser.parseArgument(args);

            HadoopPlatform platform = new HadoopPlatform(ClassifyDocsTool.class);
            // TODO VMa - figure out polling interval
//            platform.setJobPollingInterval(LOCAL_HADOOP_JOB_POLLING_INTERVAL)

            Flow trainModelFlow = TrainModelPipe.createTextFlow(platform, options);
            if (options.getDOTFile() != null) {
                trainModelFlow.writeDOT(getDotFileName(options, "make-classifier"));
                trainModelFlow.writeStepsDOT(getStepDotFileName(options, "make-classifier"));
            }
            
            trainModelFlow.complete();
        } catch (CmdLineException e) {
            System.err.println("Invalid argument: " + e);
            printUsageAndExit(parser);
        } catch (Throwable t) {
            System.err.println("Exception running tool: " + t.getMessage());
            t.printStackTrace(System.err);
            System.exit(-1);
        }

    }

}
