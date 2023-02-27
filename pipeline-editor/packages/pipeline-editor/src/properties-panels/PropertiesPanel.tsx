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

import Form, { UiSchema, Widget, AjvError } from "@rjsf/core";
import styled from "styled-components";

import {
  FileWidget,
  CustomFieldTemplate,
  ArrayTemplate,
  CustomOneOf
} from "../CustomFormControls";

export const Message = styled.div`
  margin-top: 14px;
  padding: 0 22px;
  font-family: ${({ theme }) => theme.typography.fontFamily};
  font-weight: ${({ theme }) => theme.typography.fontWeight};
  font-size: ${({ theme }) => theme.typography.fontSize};
  color: ${({ theme }) => theme.palette.text.primary};
  opacity: 0.5;
`;

const widgets: { [id: string]: Widget } = {
  file: FileWidget
};

interface Props {
  data: any;
  schema?: any;
  onChange?: (data: any) => any;
  onFileRequested?: (options: any) => any;
  onPropertiesUpdateRequested?: (options: any) => any;
}

export function PropertiesPanel({
  data,
  schema,
  onChange,
  onFileRequested,
  onPropertiesUpdateRequested
}: Props) {
  if (schema === undefined) {
    return <Message>No properties defined.</Message>;
  }
  const pipeline_input_parameters: string[] = [""];
  const uiSchema: UiSchema = {};
  for (const field in schema.properties) {
    uiSchema[field] = {};
    const properties = schema.properties[field];
    if (properties.type === "object") {
      for (const subField in properties.properties) {
        const subProps = properties.properties[subField];
        if (typeof subProps !== "boolean" && subProps.uihints) {
          uiSchema[field][subField] = subProps.uihints;
        }
      }
    }
    if (typeof properties !== "boolean" && properties.uihints) {
      uiSchema[field] = properties.uihints;
    }
  }
  let schema_copy = JSON.parse(JSON.stringify(schema));
  console.log("schema_copy")
  console.log(schema_copy)
  let pipeline_defaults = schema_copy?.properties?.pipeline_defaults?.properties
  if (pipeline_defaults) {
    if (data?.pipeline_defaults?.input_parameters) {
      for (const input_paramter of data.pipeline_defaults.input_parameters) {
        pipeline_input_parameters.push(`${input_paramter?.name}`);
      }
      pipeline_input_parameters.sort()
    }
    let kubernetes_pod_labels = 
      pipeline_defaults?.kubernetes_pod_labels?.items?.properties
        ?.value?.oneOf[1]?.properties?.value;
    if (kubernetes_pod_labels) {
      kubernetes_pod_labels.enum = pipeline_input_parameters;
    }

    let kubernetes_pod_annotations =
      pipeline_defaults?.kubernetes_pod_annotations?.items?.properties
        ?.value?.oneOf[1]?.properties?.value;
    if (kubernetes_pod_annotations) {
      kubernetes_pod_annotations.enum = pipeline_input_parameters;
    }

    let mounted_volumes =
      pipeline_defaults?.mounted_volumes?.items?.properties
        ?.pvc_name?.oneOf[1]?.properties?.value;
    if (mounted_volumes) {
      mounted_volumes.enum = pipeline_input_parameters;
    }

    let kubernetes_secrets_key =
      pipeline_defaults?.kubernetes_secrets?.items?.properties
        ?.key?.oneOf[1]?.properties?.value;
    if (kubernetes_secrets_key) {
      kubernetes_secrets_key.enum = pipeline_input_parameters;
    }

    let kubernetes_secrets_name =
      pipeline_defaults?.kubernetes_secrets?.items?.properties
        ?.name?.oneOf[1]?.properties?.value;
    if (kubernetes_secrets_name) {
      kubernetes_secrets_name.enum = pipeline_input_parameters;
    }

    let env_vars =
      pipeline_defaults?.env_vars?.items?.properties
        ?.value?.oneOf[1]?.properties?.value;
    if (env_vars) {
      env_vars.enum = pipeline_input_parameters;
    }
  }
  return (
    <Form
      formData={data}
      uiSchema={uiSchema}
      schema={schema_copy as any}
      onChange={e => {
        const newFormData = e.formData;
        const params = schema.properties?.component_parameters?.properties;
        for (const field in params) {
          if (params[field].oneOf) {
            for (const option of params[field].oneOf) {
              if (option.widget?.const !== undefined) {
                newFormData.component_parameters[field].widget =
                  option.widget.const;
              }
            }
          }
        }
        onChange?.(e.formData);
      }}
      formContext={{
        onFileRequested: async (args: any) => {
          return await onFileRequested?.({
            ...args,
            filename: undefined
          });
        },
        onPropertiesUpdateRequested: async (args: any) => {
          const newData = await onPropertiesUpdateRequested?.(args);
          onChange?.(newData);
        },
        formData: data
      }}
      id={data?.id}
      widgets={widgets}
      fields={{
        OneOfField: CustomOneOf
      }}
      liveValidate
      ArrayFieldTemplate={ArrayTemplate}
      noHtml5Validate
      FieldTemplate={CustomFieldTemplate}
      className={"elyra-formEditor"}
      transformErrors={(errors: AjvError[]) => {
        // Suppress the "oneof" validation because we're using oneOf in a custom way.
        const transformed = [];
        for (const error of errors) {
          if (
            error.message !== "should match exactly one schema in oneOf" &&
            (error as any).schemaPath?.includes("oneOf") &&
            error.message !== "should be object" &&
            error.message !== "should be string" &&
            error.message !== "should be equal to one of the allowed values"
          ) {
            transformed.push(error);
          }
        }
        return transformed;
      }}
    />
  );
}
