package org.apache.flink.table.planner.plan.nodes.exec.stream;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.flink.FlinkVersion;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeMetadata;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecAsyncCorrelate;
import org.apache.flink.table.runtime.operators.join.FlinkJoinType;
import org.apache.flink.table.types.logical.RowType;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecAsyncCorrelate.ASYNC_CORRELATE_TRANSFORMATION;

/**
 * Stream {@link ExecNode} which matches along with join a Java/Scala user defined table function.
 */
@ExecNodeMetadata(
        name = "stream-exec-async-correlate",
        version = 1,
        producedTransformations = ASYNC_CORRELATE_TRANSFORMATION,
        minPlanVersion = FlinkVersion.v2_2,
        minStateVersion = FlinkVersion.v2_2)
public class StreamExecAsyncCorrelate extends CommonExecAsyncCorrelate
        implements StreamExecNode<RowData> {

    public StreamExecAsyncCorrelate(
            ReadableConfig tableConfig,
            FlinkJoinType joinType,
            RexCall invocation,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        this(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(StreamExecAsyncCorrelate.class),
                ExecNodeContext.newPersistedConfig(StreamExecAsyncCorrelate.class, tableConfig),
                joinType,
                invocation,
                Collections.singletonList(inputProperty),
                outputType,
                description);
    }

    @JsonCreator
    public StreamExecAsyncCorrelate(
            @JsonProperty(FIELD_NAME_ID) int id,
            @JsonProperty(FIELD_NAME_TYPE) ExecNodeContext context,
            @JsonProperty(FIELD_NAME_CONFIGURATION) ReadableConfig persistedConfig,
            @JsonProperty(FIELD_NAME_JOIN_TYPE) FlinkJoinType joinType,
            @JsonProperty(FIELD_NAME_FUNCTION_CALL) RexNode invocation,
            @JsonProperty(FIELD_NAME_INPUT_PROPERTIES) List<InputProperty> inputProperties,
            @JsonProperty(FIELD_NAME_OUTPUT_TYPE) RowType outputType,
            @JsonProperty(FIELD_NAME_DESCRIPTION) String description) {
        super(
                id,
                context,
                persistedConfig,
                joinType,
                (RexCall) invocation,
                inputProperties,
                outputType,
                description);
    }
}
