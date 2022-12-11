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

async function pipelineTriggerParameters(template_name: any): Promise<any> {
  const res = await RequestHandler.makeGetRequest(
    `elyra/pipeline/pipeline_trigger/${template_name}`
  );
  return res.input_parameters;
}

let para: string[] = [];

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
  if (!pipelinePara) {
    let template_name =
      selectedNode.app_data?.component_parameters?.template_name;
    if (template_name) {
      template_name = basePath ? `${basePath}${template_name}` : template_name;
      const res = pipelineTriggerParameters(template_name);
      res.then((result: any) => {
        para = [];
        for (let item of result) {
          para = [...para, item];
        }
        setPipelinePara(true);
      });
    }
  }

  const getNodeProperties = (): any => {
    const oneOfValues: any[] = [];
    const oneOfValuesNoOpt: any[] = [];
    const filters: string[] = [""];
    const pipeline_input_parameters: string[] = [""];
    if (data?.input_parameters) {
      for (const input_paramter of data.input_parameters) {
        filters.push(`workflow.parameters.${input_paramter?.name}`);
      }
    }

    if (data?.pipeline_defaults?.input_parameters) {
      for (const input_paramter of data.pipeline_defaults.input_parameters) {
        pipeline_input_parameters.push(`${input_paramter?.name}`);
      }
    }

    oneOfValues.push(getOneOfValue(" ", " ", " "));

    // add each upstream node to the data list
    for (const upstreamNode of upstreamNodes ?? []) {
      const nodeDef = nodes.find(n => n.op === upstreamNode.op);
      const prevLen = oneOfValuesNoOpt.length;

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

      const eventFilters =
        upstreamNode.app_data?.component_parameters?.event_filter;
      if (eventFilters) {
        for (const filter of eventFilters) {
          if (
            filters.indexOf(
              `${upstreamNode.app_data?.ui_data?.label}: ${filter.name}`
            ) == -1
          ) {
            filters.push(
              `${upstreamNode.app_data?.ui_data?.label}: ${filter.name}`
            );
          }
        }
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
        const oneOfValue =
          draft.properties.component_parameters?.properties?.trigger_parameters
            ?.items?.properties?.from?.oneOf[1]?.properties?.value ||
          draft.properties.component_parameters?.properties?.init_parameters
            ?.items?.properties?.value?.oneOf[1]?.properties?.value ||
          draft.properties.component_parameters?.properties?.exit_parameters
            ?.items?.properties?.value?.oneOf[1]?.properties?.value;

        if (oneOfValue && filters.length > 0) {
          oneOfValue.enum = filters;
        }

        const allOf =
          draft.properties.component_parameters?.properties?.event_filter?.items
            ?.allOf;
        if (allOf && filters.length > 0) {
          for (let item of allOf) {
            const valueOneOf = item?.then?.properties?.value?.oneOf;
            if (valueOneOf) {
              valueOneOf[2].properties.value.enum = filters;
            }
          }
        }

        const oneOf =
          draft.properties.component_parameters?.properties?.event_filter?.items
            ?.properties?.value?.oneOf;
        if (oneOf && filters.length > 0) {
          oneOf[2].properties.value.enum = filters;
        }

        const calendar_allOf =
          draft.properties.component_parameters?.properties?.calendar?.allOf;
        if (calendar_allOf && filters.length > 0) {
          for (let item of calendar_allOf) {
            const valueOneOf = item?.then?.properties?.value?.oneOf;
            if (valueOneOf) {
              valueOneOf[2].properties.value.enum = filters;
            }
          }
        }

        const s3_object =
          draft.properties.component_parameters?.properties?.object?.properties;
        if (s3_object && filters.length > 0) {
          s3_object.prefix.oneOf[2].properties.value.enum = filters;
          s3_object.suffix.oneOf[2].properties.value.enum = filters;
          s3_object.bucket_name.oneOf[2].properties.value.enum = filters;
        }

        const dataset_oneOf =
          draft.properties.component_parameters?.properties?.dataset_name?.items
            ?.oneOf;
        if (dataset_oneOf && filters) {
          dataset_oneOf[1].properties.value.enum = filters;
        }

        // const s3_bucket_name_oneOf =
        //   draft.properties.component_parameters?.properties?.bucket_name?.oneOf;
        // if (s3_bucket_name_oneOf && filters) {
        //   s3_bucket_name_oneOf[2].properties.value.enum = filters;
        // }

        const trigger_parameters_name =
          draft.properties.component_parameters?.properties?.trigger_parameters
            ?.items?.properties?.name;
        if (
          trigger_parameters_name &&
          "enum" in trigger_parameters_name &&
          para
        ) {
          trigger_parameters_name.enum = para;
        }

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
                component_properties[prop].oneOf[
                  i
                ].properties.value.enum = pipeline_input_parameters;
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
