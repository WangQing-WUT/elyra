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

import { MetadataService, RequestHandler } from '@elyra/services';
import { URLExt } from '@jupyterlab/coreutils';
import { ServerConnection } from '@jupyterlab/services';
import { consoleIcon } from '@jupyterlab/ui-components';
import produce from 'immer';
import useSWR from 'swr';

import { PipelineService } from './PipelineService';

export const GENERIC_CATEGORY_ID = 'Elyra';

interface IReturn<T> {
  data?: T | undefined;
  error?: any;
  mutate?: any;
}

type IRuntimeImagesResponse = IRuntimeImage[];

interface IRuntimeImage {
  name: string;
  display_name: string;
  metadata: {
    image_name: string;
  };
}

const metadataFetcher = async <T>(key: string): Promise<T> => {
  return await MetadataService.getMetadata(key);
};

export const useRuntimeImages = (): IReturn<IRuntimeImagesResponse> => {
  const { data, error } = useSWR<IRuntimeImagesResponse>(
    'runtime-images',
    metadataFetcher
  );

  data?.sort((a, b) => 0 - (a.name > b.name ? -1 : 1));

  return { data, error };
};

const schemaFetcher = async <T>(key: string): Promise<T> => {
  return await MetadataService.getSchema(key);
};

// TODO: type this
export const useRuntimesSchema = (): IReturn<any> => {
  const { data, error } = useSWR<any>('runtimes', schemaFetcher);

  return { data, error };
};

interface IRuntimeComponentsResponse {
  version: string;
  categories: IRuntimeComponent[];
  properties: IComponentPropertiesResponse;
}

export interface IRuntimeComponent {
  label: string;
  image: string;
  id: string;
  description: string;
  runtime?: string;
  node_types: {
    op: string;
    id: string;
    label: string;
    image: string;
    runtime_type?: string;
    type: 'execution_node';
    inputs: { app_data: any }[];
    outputs: { app_data: any }[];
    app_data: any;
  }[];
  extensions?: string[];
}

interface IComponentPropertiesResponse {
  current_parameters: { [key: string]: any };
  parameters: { id: string }[];
  uihints: {
    parameter_info: {
      parameter_ref: string;
      control: 'custom';
      custom_control_id: string;
      label: { default: string };
      description: {
        default: string;
        placement: 'on_panel';
      };
      data: any;
    }[];
  };
  group_info: {
    group_info: {
      id: string;
      parameter_refs: string[];
    }[];
  }[];
}

/**
 * Sort palette in place. Takes a list of categories each containing a list of
 * components.
 * - Categories: alphabetically by "label" (exception: "generic" always first)
 * - Components: alphabetically by "op" (where is component label stored?)
 */
export const sortPalette = (palette: {
  categories: IRuntimeComponent[];
}): void => {
  palette.categories.sort((a, b) => {
    if (a.id === GENERIC_CATEGORY_ID) {
      return -1;
    }
    if (b.id === GENERIC_CATEGORY_ID) {
      return 1;
    }
    return a.label.localeCompare(b.label, undefined, { numeric: true });
  });

  for (const components of palette.categories) {
    components.node_types.sort((a, b) =>
      a.label.localeCompare(b.label, undefined, {
        numeric: true
      })
    );
  }
};

// TODO: This should be enabled through `extensions`
const NodeIcons: Map<string, string> = new Map([
  ['execute-notebook-node', 'static/elyra/notebook.svg'],
  ['execute-python-node', 'static/elyra/python.svg'],
  ['execute-r-node', 'static/elyra/r-logo.svg'],
  ['Events', 'static/elyra/component/category/events_category.svg'],
  ['Actions', 'static/elyra/component/category/actions_category.svg'],
  ['Logic', 'static/elyra/component/category/logic_category.svg'],
  ['Global', 'static/elyra/component/category/global_category.svg']
]);

const customIcons: Map<string, string> = new Map([
  ['Calendar Event', 'calendar_event.svg'],
  ['Dataset Event', 'dataset_event.svg'],
  ['S3 Event', 's3_event.svg'],
  ['Model Event', 'model_event.svg'],
  ['Pipeline Event', 'pipeline_event.svg'],
  ['HTTP Trigger', 'http_trigger.svg'],
  ['K8s Object Trigger', 'k8sobj_trigger.svg'],
  ['Pipeline Trigger', 'pipeline_trigger.svg'],
  ['Init', 'init.svg'],
  ['Exit', 'exit.svg'],
  ['Pipeline Branch', 'branch.svg'],
  ['Pipeline Loop', 'loop.svg']
]);
// TODO: We should decouple components and properties to support lazy loading.
// TODO: type this
export const componentFetcher = async (type: string): Promise<any> => {
  const palettePromise = RequestHandler.makeGetRequest<
    IRuntimeComponentsResponse
  >(`elyra/pipeline/components/${type}`);

  const pipelinePropertiesPromise = RequestHandler.makeGetRequest<
    IComponentPropertiesResponse
  >(`elyra/pipeline/${type}/properties`);

  const typesPromise = PipelineService.getRuntimeTypes();

  const [palette, pipelineProperties, types] = await Promise.all([
    palettePromise,
    pipelinePropertiesPromise,
    typesPromise
  ]);

  palette.properties = pipelineProperties;

  // Gather list of component IDs to fetch properties for.
  const componentList: string[] = [];
  for (const category of palette.categories) {
    for (const node of category.node_types) {
      componentList.push(node.id);
    }
  }

  const propertiesPromises = componentList.map(async componentID => {
    const res = await RequestHandler.makeGetRequest<
      IComponentPropertiesResponse
    >(`elyra/pipeline/components/${type}/${componentID}/properties`);
    return {
      id: componentID,
      properties: res
    };
  });

  // load all of the properties in parallel instead of serially
  const properties = await Promise.all(propertiesPromises);

  // inject properties
  for (const category of palette.categories) {
    // Use the runtime_type from the first node of the category to determine category
    // icon.
    // TODO: Ideally, this would be included in the category.
    const category_runtime_type =
      category.node_types?.[0]?.runtime_type ?? 'LOCAL';

    const type = types.find((t: any) => t.id === category_runtime_type);
    const baseUrl = ServerConnection.makeSettings().baseUrl;
    const defaultIcon = URLExt.parse(URLExt.join(baseUrl, type?.icon || ''))
      .pathname;
    const categoryIcon = URLExt.parse(
      URLExt.join(baseUrl, NodeIcons.get(category.id) || '')
    ).pathname;
    category.image = NodeIcons.get(category.id) ? categoryIcon : defaultIcon;

    for (const node of category.node_types) {
      // update icon
      const genericNodeIcon = NodeIcons.get(node.op);
      const nodeIcon = customIcons.has(node.label)
        ? `/static/elyra/component/${category.id}/${customIcons.get(
            node.label
          )}`
        : genericNodeIcon
        ? URLExt.parse(URLExt.join(baseUrl, genericNodeIcon)).pathname
        : defaultIcon;
      // Not sure which is needed...
      node.image = nodeIcon;
      node.app_data.image = nodeIcon;
      node.app_data.ui_data.image = nodeIcon;

      const prop = properties.find(p => p.id === node.id);
      node.app_data.properties = prop?.properties;
    }
  }
  sortPalette(palette);

  return palette;
};

const updateRuntimeImages = (
  properties: any,
  runtimeImages: IRuntimeImage[] | undefined
): void => {
  const runtimeImageProperties =
    properties?.properties?.component_parameters?.properties?.runtime_image ??
    properties?.properties?.pipeline_defaults?.properties?.runtime_image;

  const imageNames = (runtimeImages ?? []).map(i => i.metadata.image_name);

  const displayNames: { [key: string]: string } = {};

  (runtimeImages ?? []).forEach((i: IRuntimeImage) => {
    displayNames[i.metadata.image_name] = i.display_name;
  });

  if (runtimeImageProperties) {
    runtimeImageProperties.enumNames = (runtimeImages ?? []).map(
      i => i.display_name
    );
    runtimeImageProperties.enum = imageNames;
  }
};

export const usePalette = (type = 'local'): IReturn<any> => {
  const { data: runtimeImages, error: runtimeError } = useRuntimeImages();

  const { data: palette, error: paletteError, mutate: mutate } = useSWR(
    type,
    componentFetcher
  );

  let updatedPalette;
  if (palette !== undefined) {
    updatedPalette = produce(palette, (draft: any) => {
      for (const category of draft.categories) {
        for (const node of category.node_types) {
          // update runtime images
          updateRuntimeImages(node.app_data.properties, runtimeImages);
        }
      }
      updateRuntimeImages(draft.properties, runtimeImages);
    });
  }

  return {
    data: updatedPalette,
    error: runtimeError ?? paletteError,
    mutate: mutate
  };
};
