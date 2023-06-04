/*
 * Copyright 2018-2022 Elyra Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { NodeType } from "@elyra/canvas";
import produce from "immer";
import styled from "styled-components";
import { RequestHandler } from "@elyra/services";

import { PropertiesPanel, Message } from "./PropertiesPanel";
import React, { useState } from "react";
import { Collapse } from "@jupyterlab/apputils";

interface Props {
  data: any;
  basePath: any;
  selectedNodes?: any[];
  nodes: NodeType[];
  upstreamNodes?: any[];
  onFileRequested?: (options: any) => any;
  onPropertiesUpdateRequested?: (options: any) => any;
  onChange?: (nodeID: string, data: any) => any;
}

const Heading = styled.div`
  margin-top: 14px;
  padding: 0 47px;
  font-family: ${({ theme }) => theme.typography.fontFamily};
  font-weight: ${({ theme }) => theme.typography.fontWeight};
  font-size: 16px;
  color: ${({ theme }) => theme.palette.text.primary};
  opacity: 0.5;
`;

const Href = styled.div`
  padding: 0 61px;
  font-family: ${({ theme }) => theme.typography.fontFamily};
  font-weight: ${({ theme }) => theme.typography.fontWeight};
  opacity: 0.5;
`;

function getOneOfValue(value: string, option: string, label: string) {
  return {
    label: label,
    type: "object",
    properties: {
      value: {
        type: "string",
        default: value
      },
      option: {
        type: "string",
        default: option
      }
    },
    uihints: {
      value: {
        "ui:field": "hidden"
      },
      option: {
        "ui:field": "hidden"
      }
    }
  };
}

function addItem(
  eventParameters: string[],
  items: string[],
  upstreamNode: any
) {
  for (let item of items) {
    if (upstreamNode.app_data?.ui_data?.label) {
      eventParameters.push(
        `${upstreamNode.app_data?.ui_data?.label.trim()}: ${item}`
      );
    }
  }
}

async function pipelineTriggerParameters(template_name: any): Promise<any> {
  const res = await RequestHandler.makeGetRequest(
    `elyra/pipeline/pipeline_trigger/${template_name}`
  );
  return res.input_parameters;
}

let para: string[] = [];
let template_name_global: string = "";

function NodeProperties({
  data,
  basePath,
  selectedNodes,
  nodes,
  upstreamNodes,
  onFileRequested,
  onPropertiesUpdateRequested,
  onChange
}: Props) {
  const [pipelinePara, setPipelinePara] = useState(false);

  if (selectedNodes === undefined || selectedNodes.length === 0) {
    return <Message>Select a node to edit its properties.</Message>;
  }

  if (selectedNodes.length > 1) {
    return (
      <Message>
        Multiple nodes are selected. Select a single node to edit its
        properties.
      </Message>
    );
  }

  const selectedNode = selectedNodes[0];

  if (!selectedNode) {
    return <Message>Select a node to edit its properties.</Message>;
  }

  if (selectedNode.type !== "execution_node") {
    return (
      <Message>This node type doesn't have any editable properties.</Message>
    );
  }

  const nodePropertiesSchema = nodes.find(n => n.op === selectedNode.op);

  if (nodePropertiesSchema === undefined) {
    return (
      <Message>
        This node uses a component that is not stored in your component
        registry.
        {selectedNode.app_data.component_source !== undefined
          ? ` The component's path is: ${selectedNode.app_data.component_source}`
          : ""}
      </Message>
    );
  }

  if (nodePropertiesSchema?.app_data.properties === undefined) {
    return (
      <Message>This node type doesn't have any editable properties.</Message>
    );
  }
  // returns the node properties for selectedNode with the most recent content
  let template_name =
    selectedNode.app_data?.component_parameters?.template_name ||
    selectedNode.app_data?.component_parameters?.init_pipeline ||
    selectedNode.app_data?.component_parameters?.exit_pipeline;

  if (template_name != template_name_global) {
    template_name_global = template_name;
    if (template_name) {
      template_name = basePath ? `${basePath}${template_name}` : template_name;
      const res = pipelineTriggerParameters(template_name);
      res
        .then((result: any) => {
          para = [];
          for (let item of result) {
            para = [...para, item];
          }
          setPipelinePara(!pipelinePara);
        })
        .catch(error => {
          console.log(error);
          para = [];
        });
    } else {
      para = [];
    }
  }

  const getNodeProperties = (): any => {
    const oneOfValues: any[] = [];
    const oneOfValuesNoOpt: any[] = [];
    let workflowInputParameters: { [index: string]: string[] } = {
      String: [""],
      "S3 Path": [""],
      Integer: [""],
      Float: [""],
      Boolean: [""],
      List: [""]
    };
    let eventParameters: string[] = [""];
    let pipelineInputParameters: { [index: string]: string[] } = {
      String: [""],
      Integer: [""],
      Float: [""],
      Boolean: [""],
      JsonArray: [""]
    };
    let pipelineFlag = false;
    const evnetObject: { [index: string]: string[] } = {
      calendar_event: ["time"],
      model_event: ["operate"],
      dataset_event: ["operate", "samples", "symbol"],
      model_monitor_event: ["alert", "app", "model"],
      s3_event: ["bucket", "object", "operate", "size"]
    };
    if (data?.input_parameters) {
      for (const input_parameter of data.input_parameters) {
        if (
          input_parameter.type.widget in workflowInputParameters &&
          input_parameter?.name
        ) {
          workflowInputParameters[input_parameter.type.widget].push(
            `workflow.parameters.${input_parameter?.name.trim()}`
          );
        }
      }
      const sortedKeys = Object.keys(workflowInputParameters).sort();
      for (const key of sortedKeys) {
        workflowInputParameters[key].sort();
      }
    }

    if (data?.pipeline_defaults?.input_parameters) {
      pipelineFlag = true;
      for (const input_parameter of data.pipeline_defaults.input_parameters) {
        if (
          input_parameter.type.widget in pipelineInputParameters &&
          input_parameter?.name
        ) {
          pipelineInputParameters[input_parameter.type.widget].push(
            `${input_parameter?.name}`
          );
        }
      }
      const sortedKeys = Object.keys(pipelineInputParameters).sort();
      for (const key of sortedKeys) {
        pipelineInputParameters[key].sort();
      }
    }

    oneOfValues.push(getOneOfValue(" ", " ", " "));

    // add each upstream node to the data list
    for (const upstreamNode of upstreamNodes ?? []) {
      const nodeDef = nodes.find(n => n.op === upstreamNode.op);
      const prevLen = oneOfValuesNoOpt.length;

      if (upstreamNode.op.indexOf("loop_start") != -1) {
        const loop_args = upstreamNode.app_data.component_parameters?.loop_args;
        const oneOfValue = getOneOfValue(
          upstreamNode.id,
          `ParallelFor:item`,
          `${upstreamNode.app_data?.ui_data?.label.trim()}: item`
        );
        oneOfValues.push(oneOfValue);
        if (loop_args?.widget == "List[Dict[str, any]]") {
          if (loop_args?.value?.[0]) {
            for (let item of loop_args.value[0]) {
              if ("key" in item) {
                const oneOfValue = getOneOfValue(
                  upstreamNode.id,
                  `ParallelFor:item.${item.key}`,
                  `${upstreamNode.app_data?.ui_data?.label.trim()}: item.${
                    item.key
                  }`
                );
                oneOfValues.push(oneOfValue);
              }
            }
          }
        }
      }

      const nodeProperties =
        nodeDef?.app_data.properties.properties.component_parameters.properties;
      // Add each property with a format of outputpath to the options field
      for (const prop in nodeProperties ?? {}) {
        const properties = nodeProperties[prop] ?? {};
        if (properties.uihints?.outputpath) {
          // Creates a "oneof" object for each node / output pair
          const oneOfValue = getOneOfValue(
            upstreamNode.id,
            prop,
            `${upstreamNode.app_data?.ui_data?.label}: ${properties.title}`
          );
          oneOfValues.push(oneOfValue);
          oneOfValuesNoOpt.push(oneOfValue);
        }
      }
      const nodeType: string = upstreamNode.op.split(":")[0];
      if (nodeType in evnetObject) {
        addItem(eventParameters, evnetObject[nodeType], upstreamNode);
      }

      if (oneOfValuesNoOpt.length <= prevLen) {
        // Add oneOfValue for parent without specified outputs
        oneOfValuesNoOpt.push(
          getOneOfValue(
            upstreamNode.id,
            "",
            upstreamNode.app_data?.ui_data?.label
          )
        );
      }
    }
    eventParameters.sort();
    oneOfValues.sort((a, b) => (a.label > b.label ? 1 : -1));

    // update property data to include data for properties with inputpath format

    return produce(
      nodePropertiesSchema?.app_data?.properties ?? {},
      (draft: any) => {
        draft.properties = {
          label: {
            title: "Label",
            description: "A custom label for the node.",
            type: "string"
          },
          ...draft.properties
        };

        const kubernetes_pod_labels =
          draft.properties.component_parameters?.properties
            ?.kubernetes_pod_labels?.items?.properties?.value?.oneOf[1]
            ?.properties?.value;
        if (kubernetes_pod_labels) {
          kubernetes_pod_labels.enum = pipelineInputParameters["String"];
        }

        const kubernetes_pod_annotations =
          draft.properties.component_parameters?.properties
            ?.kubernetes_pod_annotations?.items?.properties?.value?.oneOf[1]
            ?.properties?.value;
        if (kubernetes_pod_annotations) {
          kubernetes_pod_annotations.enum = pipelineInputParameters["String"];
        }

        const mounted_volumes =
          draft.properties.component_parameters?.properties?.mounted_volumes
            ?.items?.properties?.pvc_name?.oneOf[1]?.properties?.value;
        if (mounted_volumes) {
          mounted_volumes.enum = pipelineInputParameters["String"];
        }

        const kubernetes_secrets_key =
          draft.properties.component_parameters?.properties?.kubernetes_secrets
            ?.items?.properties?.key?.oneOf[1]?.properties?.value;
        if (kubernetes_secrets_key) {
          kubernetes_secrets_key.enum = pipelineInputParameters["String"];
        }

        const kubernetes_secrets_name =
          draft.properties.component_parameters?.properties?.kubernetes_secrets
            ?.items?.properties?.name?.oneOf[1]?.properties?.value;
        if (kubernetes_secrets_name) {
          kubernetes_secrets_name.enum = pipelineInputParameters["String"];
        }

        const env_vars =
          draft.properties.component_parameters?.properties?.env_vars?.items
            ?.properties?.value?.oneOf[1]?.properties?.value;
        if (env_vars) {
          env_vars.enum = pipelineInputParameters["String"];
        }
        // workflow input parameters placeholder of trigger parameters、init and exit
        const workflowOneOfValue =
          draft.properties.component_parameters?.properties?.trigger_parameters
            ?.items?.properties?.from?.oneOf[1]?.properties?.value ||
          draft.properties.component_parameters?.properties?.init_parameters
            ?.items?.properties?.value?.oneOf[1]?.properties?.value ||
          draft.properties.component_parameters?.properties?.exit_parameters
            ?.items?.properties?.value?.oneOf[1]?.properties?.value;

        if (workflowOneOfValue) {
          const trigger_para = workflowInputParameters["String"]
            .concat(workflowInputParameters["Boolean"])
            .concat(workflowInputParameters["Integer"])
            .concat(workflowInputParameters["Float"])
            .concat(workflowInputParameters["List"])
            .concat(["workflow.instance_name"])
            .filter((value, index, self) => {
              return self.indexOf(value) === index;
            })
            .sort();
          workflowOneOfValue.enum = trigger_para;
        }
        // events parameters placeholder of trigger parameters
        const eventOneOfValue =
          draft.properties.component_parameters?.properties?.trigger_parameters
            ?.items?.properties?.from?.oneOf[2]?.properties?.value;

        if (eventOneOfValue) {
          eventOneOfValue.enum = eventParameters;
        }

        // workflow input parameters placeholder of events filter
        const allOf =
          draft.properties.component_parameters?.properties?.event_filter?.items
            ?.allOf;
        if (allOf) {
          for (let item of allOf) {
            const valueOneOf = item?.then?.properties?.value?.oneOf;
            const paraName = item?.if?.properties?.name?.const;
            if (valueOneOf) {
              if (paraName == "samples") {
                valueOneOf[2].properties.value.enum =
                  workflowInputParameters["Integer"];
              } else {
                valueOneOf[2].properties.value.enum =
                  workflowInputParameters["String"];
              }
            }
          }
        }

        // workflow input parameters placeholder of calendar events filter
        const calendar_allOf =
          draft.properties.component_parameters?.properties?.calendar?.allOf;
        if (calendar_allOf) {
          for (let item of calendar_allOf) {
            const valueOneOf = item?.then?.properties?.value?.oneOf;
            if (valueOneOf) {
              valueOneOf[1].properties.value.enum =
                workflowInputParameters["String"];
            }
          }
        }

        // workflow input parameters placeholder of datasets or models
        const dataset_or_model_oneOf =
          draft.properties.component_parameters?.properties?.dataset_name?.items
            ?.oneOf ||
          draft.properties.component_parameters?.properties?.model_name?.items
            ?.oneOf;
        if (dataset_or_model_oneOf) {
          const s3_para = workflowInputParameters["String"]
            .concat(workflowInputParameters["S3 Path"])
            .filter((value, index, self) => {
              return self.indexOf(value) === index;
            })
            .sort();
          dataset_or_model_oneOf[1].properties.value.enum = s3_para;
        }

        // workflow input parameters placeholder of appNames or modelName of model monitor event
        const app_name =
          draft.properties.component_parameters?.properties?.event_filter?.items
            ?.properties?.app_name;
        const model_name =
          draft.properties.component_parameters?.properties?.event_filter?.items
            ?.properties?.model_name;
        if (app_name && model_name) {
          app_name.oneOf[2].properties.value.enum =
            workflowInputParameters["String"];
          model_name.oneOf[2].properties.value.enum =
            workflowInputParameters["String"];
        }

        // pipeline input parameters placeholder of condition
        const branch_parameter1 =
          draft.properties.component_parameters?.properties?.branch_conditions
            ?.properties?.branch_parameter1;
        const branch_parameter2 =
          draft.properties.component_parameters?.properties?.branch_conditions
            ?.properties?.branch_parameter2;
        if (branch_parameter1 && branch_parameter2) {
          const branch_para = pipelineInputParameters["String"]
            .concat(pipelineInputParameters["Boolean"])
            .concat(pipelineInputParameters["Integer"])
            .concat(pipelineInputParameters["Float"])
            .filter((value, index, self) => {
              return self.indexOf(value) === index;
            })
            .sort();
          branch_parameter1.oneOf[0].properties.value.enum = branch_para;
          branch_parameter1.oneOf[1].properties.value.oneOf = oneOfValues;
          branch_parameter1.oneOf[1].properties.value.type = "object";
          delete branch_parameter1.oneOf[1].properties.value.enum;
          delete branch_parameter1.oneOf[1].properties.value.default;
          branch_parameter2.oneOf[1].properties.value.enum = branch_para;
          branch_parameter2.oneOf[2].properties.value.oneOf = oneOfValues;
          branch_parameter2.oneOf[2].properties.value.type = "object";
          delete branch_parameter2.oneOf[2].properties.value.enum;
          delete branch_parameter2.oneOf[2].properties.value.default;
        }

        // pipeline parameter names placeholder of pipeline trigger, init and exit
        const parameters_name =
          draft.properties.component_parameters?.properties?.trigger_parameters
            ?.items?.properties?.name ||
          draft.properties.component_parameters?.properties?.init_parameters
            ?.items?.properties?.name ||
          draft.properties.component_parameters?.properties?.exit_parameters
            ?.items?.properties?.name;
        if (parameters_name && "enum" in parameters_name && para) {
          parameters_name.enum = para;
        }

        // pipeline component parameter placeholder
        const component_properties =
          draft.properties.component_parameters?.properties ?? {};
        for (let prop in component_properties) {
          if (
            component_properties[prop].properties?.value &&
            component_properties[prop].properties?.widget
          ) {
            const properties = component_properties[prop].properties;
            const oneOf = properties.value?.uihints?.allownooptions
              ? oneOfValuesNoOpt
              : oneOfValues;
            component_properties[prop].required = ["value"];
            if (
              properties?.widget?.default === "inputpath" &&
              properties.value
            ) {
              if (oneOf.length > 0) {
                properties.value.oneOf = oneOf;
                delete properties.value.enum;
                delete properties.value.type;
              }
            }
          } else if (component_properties[prop].oneOf) {
            for (const i in component_properties[prop].oneOf) {
              const nestedOneOf = component_properties[prop].oneOf[i].uihints
                ?.allownooptions
                ? oneOfValuesNoOpt
                : oneOfValues;
              component_properties[prop].oneOf[i].required = ["value"];
              if (
                component_properties[prop].oneOf[i].properties?.value
                  ?.default === undefined &&
                component_properties[prop].oneOf[i].properties?.value?.type ===
                  "string"
              ) {
                component_properties[prop].oneOf[i].properties.value.default =
                  "";
              }
              if (
                component_properties[prop].oneOf[i].properties.widget
                  .default === "enum"
              ) {
                if (pipelineFlag) {
                  if (
                    pipelineInputParameters[
                      component_properties[prop]?.type_desc
                    ]
                  ) {
                    component_properties[prop].oneOf[i].properties.value.enum =
                      pipelineInputParameters[
                        component_properties[prop]?.type_desc
                      ];
                  } else if (prop == "loop_args") {
                    component_properties[prop].oneOf[i].properties.value.enum =
                      pipelineInputParameters["JsonArray"];
                  } else {
                    component_properties[prop].oneOf[
                      i
                    ].properties.value.enum = [""];
                  }
                } else {
                  component_properties[prop].oneOf[i].properties.value.enum =
                    workflowInputParameters["String"];
                }
              } else if (
                component_properties[prop].oneOf[i].properties.widget
                  .default === "inputpath"
              ) {
                if (nestedOneOf.length > 0) {
                  component_properties[prop].oneOf[
                    i
                  ].properties.value.oneOf = nestedOneOf;
                  component_properties[prop].oneOf[i].properties.value.type =
                    "object";
                  delete component_properties[prop].oneOf[i].properties.value
                    .enum;
                  delete component_properties[prop].oneOf[i].properties.value
                    .default;
                }
              }
            }
          }
        }
      }
    );
  };

  if (selectedNode.op.indexOf("calendar_event") != -1) {
    return (
      <div>
        <Heading>{nodePropertiesSchema.label}</Heading>
        <span className="nodeDescription">
          {nodePropertiesSchema.description}
        </span>
        <PropertiesPanel
          key={selectedNode.id}
          schema={getNodeProperties()}
          data={selectedNode.app_data}
          onChange={(data: any) => {
            onChange?.(selectedNode.id, data);
          }}
          onFileRequested={onFileRequested}
          onPropertiesUpdateRequested={onPropertiesUpdateRequested}
        />
        <Href>
          <a href="https://crontab.guru/" target="_blank">
            crontab.guru
          </a>
        </Href>
      </div>
    );
  }

  return (
    <div>
      <Heading>{nodePropertiesSchema.label}</Heading>
      <span className="nodeDescription">
        {nodePropertiesSchema.description}
      </span>
      <PropertiesPanel
        key={selectedNode.id}
        schema={getNodeProperties()}
        data={selectedNode.app_data}
        onChange={(data: any) => {
          onChange?.(selectedNode.id, data);
        }}
        onFileRequested={onFileRequested}
        onPropertiesUpdateRequested={onPropertiesUpdateRequested}
      />
    </div>
  );
}

export default NodeProperties;
