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

import { IEditorServices } from '@jupyterlab/codeeditor';
import { TranslationBundle } from '@jupyterlab/translation';
import { IFormComponentRegistry } from '@jupyterlab/ui-components';
import Form, {
  ArrayFieldTemplateProps,
  FieldTemplateProps,
  IChangeEvent,
  Widget
} from '@rjsf/core';

import { showBrowseFileDialog } from './BrowseFileDialog';
import { PathExt } from '@jupyterlab/coreutils';
import * as React from 'react';

/**
 * Props passed to the FormEditor.
 */
interface IFormEditorProps {
  /**
   * Schema for form fields to be displayed.
   */
  schema: any;

  /**
   * Handler to update form data / error state in parent component.
   */
  onChange: (formData: any, invalid: boolean) => void;

  /**
   * Editor services to create new editors in code fields.
   */
  editorServices: IEditorServices | null;

  /**
   * Translator for internationalization
   */
  translator: TranslationBundle;

  /**
   * Registry to retrieve custom renderers.
   */
  componentRegistry?: IFormComponentRegistry;

  /**
   * Metadata that already exists (if there is any)
   */
  originalData?: any;

  /**
   * All existing tags to give as options.
   */
  allTags?: string[];

  /**
   * All existing languages to give as options.
   */
  languageOptions?: string[];

  browserFactory: any;
}

/**
 * React component that allows for custom add / remove buttons in the array
 * field component.
 */
const ArrayTemplate: React.FC<ArrayFieldTemplateProps> = props => {
  return (
    <div className={props.className}>
      {props.items.map(item => {
        if (item.hasRemove) {
          return (
            <div key={item.key} className={item.className}>
              {item.children}
              <button
                className="jp-mod-styled jp-mod-warn"
                onClick={item.onDropIndexClick(item.index)}
                disabled={!item.hasRemove}
              >
                {props.formContext.trans.__('Remove')}
              </button>
            </div>
          );
        } else {
          return (
            <div key={item.key} className={item.className}>
              {item.children}
            </div>
          );
        }
      })}
      {props.canAdd && (
        <button
          className="jp-mod-styled jp-mod-reject"
          onClick={props.onAddClick}
        >
          {props.formContext.trans.__('Add') ?? 'Add'}
        </button>
      )}
    </div>
  );
};

const CustomFieldTemplate: React.FC<FieldTemplateProps> = props => {
  return (
    <div className={props.classNames}>
      {props.schema.title !== undefined && props.schema.title !== ' ' ? (
        <div className="label-header">
          <label className="control-label" htmlFor={props.id}>
            {`${props.schema.title}${props.required ? '*' : ''}`}
          </label>
          {props.schema.description && (
            <div className="description-wrapper">
              <div className="description-button">?</div>
              <p
                className={`field-description ${
                  props.schema.title.length < 10 ? 'short-title' : ''
                }`}
              >
                {props.schema.description}
              </p>
            </div>
          )}
        </div>
      ) : (
        undefined
      )}
      {props.children}
      {props.errors}
    </div>
  );
};

const FileWidget: Widget = props => {
  const handleChooseFile = React.useCallback(async () => {
    const values = await props.formContext.onFileRequested({
      canSelectMany: false,
      defaultUri: props.value,
      filters: { File: props.uiSchema.extensions },
      propertyID: props.id.replace('root_component_parameters_', '')
    });
    if (values?.[0]) {
      props.onChange(values[0]);
    }
  }, [props]);

  return (
    <div id={props.id} style={{ display: 'flex' }}>
      <input
        type="text"
        className="form-control"
        value={props.value ?? ''}
        placeholder={props.uiSchema?.['ui:placeholder']}
        onChange={e => {
          console.log(e);
        }}
        disabled
      />
      <button
        className="form-control"
        style={{ width: 'fit-content' }}
        onClick={handleChooseFile}
      >
        Browse
      </button>
    </div>
  );
};

const widgets: { [id: string]: Widget } = {
  file: FileWidget
};

function getPipelineRelativeNodePath(
  pipelinePath: string,
  nodePath: string
): string {
  const relativePath: string = PathExt.relative(
    PathExt.dirname(pipelinePath),
    nodePath
  );
  return relativePath;
}

/**
 * React component that wraps the RJSF form editor component.
 * Creates a uiSchema from given uihints and passes relevant information
 * to the custom renderers.
 */
export const FormEditor: React.FC<IFormEditorProps> = ({
  schema,
  onChange,
  editorServices,
  componentRegistry,
  translator,
  originalData,
  allTags,
  languageOptions,
  browserFactory
}) => {
  const [formData, setFormData] = React.useState(originalData ?? ({} as any));

  /**
   * Generate the rjsf uiSchema from uihints in the elyra metadata schema.
   */
  const uiSchema: any = {
    classNames: 'elyra-formEditor'
  };
  for (const category in schema?.properties) {
    const properties = schema.properties[category];
    uiSchema[category] = {};
    for (const field in properties.properties) {
      uiSchema[category][field] = properties.properties[field].uihints ?? {};
      uiSchema[category][field].classNames = `elyra-formEditor-form-${field}`;
    }
  }

  const onFileRequested = async (
    flag: boolean
  ): Promise<string[] | undefined> => {
    const res = await showBrowseFileDialog(
      browserFactory.defaultBrowser.model.manager,
      {
        startPath: '',
        includeDir: flag,
        filter: (model: any): boolean => {
          const ext = PathExt.extname(model.path);
          if (ext == '') {
            return true;
          } else {
            if (flag) {
              return false;
            } else {
              return ['.yaml'].includes(ext);
            }
          }
        }
      }
    );

    if (res.button.accept && res.value.length) {
      const file = getPipelineRelativeNodePath('/', res.value[0].path);
      return [file];
    }
    return undefined;
  };

  let flag = true;
  if (schema?.properties?.Configuration) {
    flag = false;
  }

  return (
    <Form
      schema={schema}
      formData={formData}
      formContext={{
        onFileRequested: async (args: any) => {
          return await onFileRequested?.(flag);
        },
        editorServices: editorServices,
        language: formData?.['Source']?.language ?? '',
        allTags: allTags,
        languageOptions: languageOptions,
        trans: translator
      }}
      widgets={widgets}
      fields={componentRegistry?.renderers}
      ArrayFieldTemplate={ArrayTemplate}
      FieldTemplate={CustomFieldTemplate}
      uiSchema={uiSchema}
      onChange={(e: IChangeEvent<any>): void => {
        setFormData(e.formData);
        onChange(e.formData, e.errors.length > 0 || false);
      }}
      liveValidate={true}
      noHtml5Validate={
        /** noHtml5Validate is set to true to prevent the html validation from moving the focus when the live validate is called. */
        true
      }
    />
  );
};
