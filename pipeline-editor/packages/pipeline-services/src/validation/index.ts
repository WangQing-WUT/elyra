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

import { parseTree, findNodeAtLocation } from "jsonc-parser";

import checkCircularReferences from "./check-circular-references";
import { PartialProblem, Problem } from "./types";
import {
  findNode,
  getLinks,
  getNodes,
  getValue,
  rangeForLocation
} from "./utils";
import {
  getErrorMessages,
  getEnumValidators,
  getNestedEnumValidators,
  getNumberValidators,
  getStringValidators,
  getStringArrayValidators
} from "./validators";

export function getLinkProblems(pipeline: any) {
  const links = getLinks(pipeline);

  const taintedLinks = checkCircularReferences(links);

  let problems: PartialProblem[] = [];
  for (const linkID of taintedLinks) {
    // linkID should be guaranteed to be found.
    const link = links.find(l => l.id === linkID)!;

    const source = findNode(pipeline, link.srcNodeId);
    const target = findNode(pipeline, link.trgNodeId);
    problems.push({
      message: `The connection between nodes '${source.app_data.ui_data.label}' and '${target.app_data.ui_data.label}' is part of a circular reference.`,
      path: [...link.path, "id"],
      info: {
        type: "circularReference",
        pipelineID: pipeline.id,
        linkID: linkID
      }
    });
  }

  return problems;
}

// TODO: Update this to validate the new schema format
function getPropertyValidationErrors(prop: any, value: any): [any[], string] {
  let errorMessages: any[] = [];
  let propName = "";
  switch (prop.title) {
    case "Calendar Event Filters":
      if (value.value.widget === "string") {
        const trimmedString = (value.value.value ?? "").trim();
        if (trimmedString === "") {
          break;
        }
        propName = value.name;
        const stringValidators = getStringValidators(prop.uihints[propName]);
        errorMessages = getErrorMessages(trimmedString, stringValidators);
      }
  }

  switch (prop.custom_control_id) {
    case "EnumControl":
      const enumValidators = getEnumValidators(prop.data);
      errorMessages = getErrorMessages(value, enumValidators);
      break;
    case "NestedEnumControl":
      const nestedEnumValidators = getNestedEnumValidators(prop.data);
      errorMessages = prop.data?.required
        ? getErrorMessages(value, nestedEnumValidators)
        : [];
      break;
    case "NumberControl":
      const trimmedNumber = (value?.toString() ?? "").trim();
      if (!prop.data?.required && trimmedNumber === "") {
        break;
      }
      const numberValidators = getNumberValidators(prop.data);
      errorMessages = getErrorMessages(trimmedNumber, numberValidators);
      break;
    case "StringArrayControl":
      const stringArrayValidators = getStringArrayValidators(prop.data);
      errorMessages = getErrorMessages(value ?? [], stringArrayValidators);
      break;
    case "StringControl":
      const trimmedString = (value ?? "").trim();
      if (!prop.data?.required && trimmedString === "") {
        break;
      }
      const stringValidators = getStringValidators(prop.data);
      errorMessages = getErrorMessages(trimmedString, stringValidators);
      break;
  }
  return [errorMessages, propName];
}

export function getPipelineProblems(pipeline: any, pipelineProperties: any) {
  let problems: PartialProblem[] = [];

  for (const fieldName in pipelineProperties?.properties ?? []) {
    // If the property isn't in the json, report the error one level higher.
    let path = ["pipeline", "0", "app_data"];
    if (pipeline.app_data?.[fieldName] !== undefined) {
      path.push(pipelineProperties[fieldName]);
    }

    const prop = pipelineProperties.properties[fieldName];

    // this should be safe because a boolean can't be required
    // otherwise we would need to check strings for undefined or empty string
    // NOTE: 0 is also falsy, but we don't have any number inputs right now?
    // TODO: We should update this to do type checking.
    const value = getValue(
      pipeline.app_data?.properties,
      fieldName,
      pipeline.app_data?.properties?.pipeline_defaults
    );
    if (prop.data?.required && !value) {
      problems.push({
        message: `The pipeline property '${prop.title}' is required.`,
        path,
        info: {
          type: "missingProperty",
          pipelineID: pipeline.id,
          // do not strip elyra here, we need to differentiate between pipeline_defaults still.
          property: fieldName
        }
      });
    }

    let [errorMessages, propName] = getPropertyValidationErrors(prop, value);

    if (errorMessages[0] !== undefined) {
      problems.push({
        message: `The pipeline property '${prop.title}' is invalid: ${errorMessages[0]}`,
        path,
        info: {
          type: "invalidProperty",
          pipelineID: pipeline.id,
          // do not strip elyra here, we need to differentiate between pipeline_defaults still.
          property: fieldName,
          message: errorMessages[0]
        }
      });
    }
  }

  return problems;
}

export function getNodeProblems(pipeline: any, nodeDefinitions: any) {
  const nodes = getNodes(pipeline);
  let problems: PartialProblem[] = [];
  let init = 0;
  let exit = 0;
  for (const [n, node] of nodes.entries()) {
    if (node.type !== "execution_node") {
      continue;
    }

    const nodeDef = nodeDefinitions.find((n: any) => n.op === node.op);
    if (nodeDef === undefined) {
      problems.push({
        message: `The component '${node.op}' cannot be found.`,
        path: ["nodes", n],
        info: {
          type: "missingComponent",
          pipelineID: pipeline.id,
          nodeID: node.id,
          op: node.op
        }
      });
      continue;
    }

    const nodeLabel = node.app_data?.label;
    const nodeOp = node.op;
    let path = ["nodes", n, "app_data"];

    if (nodeOp.indexOf("init") != -1) {
      if (init === 0) {
        init++;
      } else {
        problems.push({
          message: `Canvas can only contain one Init component.`,
          path,
          info: {
            type: "invalidComponent",
            pipelineID: pipeline.id,
            nodeID: node.id,
            message: "Canvas can only contain one Init component"
          }
        });
      }
    }

    if (nodeOp.indexOf("exit") != -1) {
      if (exit === 0) {
        exit++;
      } else {
        problems.push({
          message: `Canvas can only contain one Exit component.`,
          path,
          info: {
            type: "invalidComponent",
            pipelineID: pipeline.id,
            nodeID: node.id,
            message: "Canvas can only contain one Exit component"
          }
        });
      }
    }

    if (
      nodeOp.indexOf("execute") == -1 &&
      nodeOp.indexOf("catalog") == -1 &&
      nodeOp.indexOf("loop") == -1 &&
      nodeOp.indexOf("branch") == -1 &&
      nodeOp.indexOf("init") == -1 &&
      nodeOp.indexOf("exit") == -1
    ) {
      if (!nodeLabel) {
        problems.push({
          message: `The property 'Name' on node '${node.app_data.ui_data.label}' is required.`,
          path,
          info: {
            type: "missingProperty",
            pipelineID: pipeline.id,
            nodeID: node.id,
            property: "Name"
          }
        });
      } else {
        const rExp: RegExp = /^[a-z][a-z0-9-]*[a-z0-9]$/;
        if (!rExp.test(nodeLabel)) {
          problems.push({
            message: `The property 'Name' on node '${node.app_data.ui_data.label}' is invalid: The field can only contain lowercase letters, numbers and '-'.`,
            path,
            info: {
              type: "invalidProperty",
              pipelineID: pipeline.id,
              nodeID: node.id,
              property: "Name",
              message:
                "The field can only contain lowercase letters, numbers and '-'"
            }
          });
        }
        if (nodeOp.indexOf("trigger") != -1) {
          if (nodeLabel.length > 23) {
            problems.push({
              message: `The length of trigger name should be less than 23 characters.`,
              path,
              info: {
                type: "invalidProperty",
                pipelineID: pipeline.id,
                nodeID: node.id,
                property: "Name",
                message:
                  "The length of trigger name should be less than 23 characters."
              }
            });
          }
        }
      }
    } else if (nodeOp.indexOf("branch") != -1 && nodeLabel) {
      const rExp: RegExp = /^[a-z][a-z0-9-]*[a-z0-9]$/;
      if (!rExp.test(nodeLabel)) {
        problems.push({
          message: `The property 'Label' on node '${node.app_data.ui_data.label}' is invalid: The field can only contain lowercase letters, numbers and '-'.`,
          path,
          info: {
            type: "invalidProperty",
            pipelineID: pipeline.id,
            nodeID: node.id,
            property: "Label",
            message:
              "The field can only contain lowercase letters, numbers and '-'"
          }
        });
      }
    }

    const nodeProperties =
      nodeDef.app_data.properties?.properties?.component_parameters?.properties;
    for (const fieldName in nodeProperties ?? []) {
      const prop = nodeProperties[fieldName];
      // If the property isn't in the json, report the error one level higher.
      let path = ["nodes", n, "app_data"];
      if (node.app_data.component_parameters?.[fieldName] !== undefined) {
        path.push(prop.parameter_ref);
      }

      // this should be safe because a boolean can't be required
      // otherwise we would need to check strings for undefined or empty string
      // NOTE: 0 is also falsy, but we don't have any number inputs right now?
      // TODO: We should update this to do type checking.
      const value = getValue(
        node.app_data.component_parameters ?? {},
        fieldName,
        pipeline.app_data?.properties?.pipeline_defaults
      );
      const component_parameters =
        nodeDef.app_data.properties?.properties?.component_parameters ?? {};

      if (component_parameters.required?.includes(fieldName)) {
        if (
          !value ||
          (Array.prototype.isPrototypeOf(value) && value.length === 0) ||
          (value?.widget && (!value.value || value.value === "")) ||
          (value?.value?.widget &&
            (!value.value.value || value.value.value === ""))
        ) {
          problems.push({
            message: `The property '${prop.title}' on node '${node.app_data.ui_data.label}' is required.`,
            path,
            info: {
              type: "missingProperty",
              pipelineID: pipeline.id,
              nodeID: node.id,
              property: fieldName
            }
          });
        }
      }

      let [errorMessages, propName] = getPropertyValidationErrors(prop, value);

      if (errorMessages[0] !== undefined) {
        problems.push({
          message: `The property '${prop.title}' on node '${node.app_data.ui_data.label}' is invalid: ${errorMessages[0]}`,
          path,
          info: {
            type: "invalidProperty",
            pipelineID: pipeline.id,
            nodeID: node.id,
            // do not strip elyra here, we need to differentiate between component_parameters still.
            property: prop.parameter_ref ? prop.parameter_ref : propName,
            message: errorMessages[0]
          }
        });
      }
    }
  }
  return problems;
}

export function validate(
  pipeline: string,
  nodeDefinitions: any,
  pipelineProperties?: any
) {
  const pipelineTreeRoot = parseTree(pipeline);
  if (pipelineTreeRoot === undefined) {
    return [];
  }

  const pipelineJSON = JSON.parse(pipeline);

  let problems: Problem[] = [];
  for (const [p, pipeline] of pipelineJSON.pipelines.entries()) {
    let partials: PartialProblem[] = [];

    partials.push(...getPipelineProblems(pipeline, pipelineProperties));
    partials.push(...getLinkProblems(pipeline));
    partials.push(...getNodeProblems(pipeline, nodeDefinitions));

    problems.push(
      ...partials.map(partial => {
        const { path, ...rest } = partial;
        const location = findNodeAtLocation(pipelineTreeRoot, [
          "pipelines",
          p,
          ...path
        ]);
        return {
          ...rest,
          severity: 1 as 1 | 2 | 3 | 4 | undefined,
          range: rangeForLocation(location)
        };
      })
    );
  }

  return problems;
}

export * from "./validators";
export * from "./types";
