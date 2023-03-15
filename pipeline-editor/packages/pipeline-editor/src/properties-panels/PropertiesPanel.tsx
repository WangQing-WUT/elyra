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

  const selectPipelineParameters = (): any => {
    if (schema?.properties?.pipeline_defaults?.properties) {
      const input_parameters = data?.pipeline_defaults?.input_parameters;
      const pipeline_input_parameters: string[] = [""];
      let schema_copy = JSON.parse(JSON.stringify(schema));
      let pipeline_defaults =
        schema_copy.properties.pipeline_defaults.properties;
      for (let index in input_parameters) {
        if (input_parameters[index]?.name) {
          pipeline_input_parameters.push(
            `${input_parameters[index]?.name.trim()}`
          );
        }
      }
      pipeline_input_parameters.sort();

      pipeline_defaults.kubernetes_pod_labels.items.properties.value.oneOf[1].properties.value.enum = pipeline_input_parameters;

      pipeline_defaults.kubernetes_pod_annotations.items.properties.value.oneOf[1].properties.value.enum = pipeline_input_parameters;

      pipeline_defaults.mounted_volumes.items.properties.pvc_name.oneOf[1].properties.value.enum = pipeline_input_parameters;

      pipeline_defaults.kubernetes_secrets.items.properties.key.oneOf[1].properties.value.enum = pipeline_input_parameters;

      pipeline_defaults.kubernetes_secrets.items.properties.name.oneOf[1].properties.value.enum = pipeline_input_parameters;

      pipeline_defaults.env_vars.items.properties.value.oneOf[1].properties.value.enum = pipeline_input_parameters;
      return schema_copy;
    } else {
      return schema;
    }
  };
  let flag = 0;
  let formData: any;
  return (
    <Form
      formData={data}
      uiSchema={uiSchema}
      schema={selectPipelineParameters()}
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

        formData = e.formData;
        if (flag === 1) {
          onChange?.(e.formData);
          flag = 0;
        }
      }}
      onBlur={() => {
        onChange?.(formData);
      }}
      formContext={{
        onFileRequested: async (args: any) => {
          let filename = await onFileRequested?.({
            ...args,
            filename: undefined
          });
          flag = 1;
          return filename;
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
