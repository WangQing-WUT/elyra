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

import {
  MetadataWidget,
  IMetadataWidgetProps,
  IMetadata,
  MetadataDisplay,
  IMetadataDisplayProps,
  IMetadataDisplayState,
  IMetadataActionButton
} from '@elyra/metadata-common';
import { IDictionary } from '@elyra/services';
import { RequestErrors } from '@elyra/ui-components';
import { JupyterFrontEnd } from '@jupyterlab/application';
import { IThemeManager } from '@jupyterlab/apputils';
import { LabIcon, refreshIcon } from '@jupyterlab/ui-components';

import React from 'react';
import { PassThrough } from 'stream';

import { PipelineService } from './PipelineService';

export const COMPONENT_CATALOGS_SCHEMASPACE = 'component-catalogs';

const COMPONENT_CATALOGS_CLASS = 'elyra-metadata-component-catalogs';

const handleError = (error: any): void => {
  // silently eat a 409, the server will log in in the console
  if (error.status !== 409) {
    RequestErrors.serverError(error);
  }
};

const commands = {
  OPEN_COMPONENT_EDITOR: 'elyra-component-editor:open'
};
/**
 * A React Component for displaying the component catalogs list.
 */
class ComponentCatalogsDisplay extends MetadataDisplay<
  IMetadataDisplayProps,
  IMetadataDisplayState
> {
  actionButtons(metadata: IMetadata): IMetadataActionButton[] {
    return [
      {
        title: 'Reload components from catalog',
        icon: refreshIcon,
        onClick: (): void => {
          PipelineService.refreshComponentsCache(metadata.name)
            .then((response: any): void => {
              this.props.updateMetadata();
            })
            .catch(error => handleError(error));
        }
      },
      ...super.actionButtons(metadata)
    ];
  }

  openComponentEditor = (args: any): void => {
    this.props.commands.execute(commands.OPEN_COMPONENT_EDITOR, args);
  };
  // render catalog entries
  renderExpandableContent(metadata: IDictionary<any>): JSX.Element {
    // let category_output = <li key="No category">No category</li>;
    // if (metadata.metadata.categories) {
    //   category_output = metadata.metadata.categories.map((category: string) => (
    //     <li key={category}>{category}</li>
    //   ));
    // }

    let component_output = <li key="No category">No category</li>;
    if (metadata.metadata.paths) {
      component_output = metadata.metadata.paths.map((path: string) => (
        <li>
          <div className="elyra-expandableContainer-title">
            <span className="elyra-expandableContainer-name elyra-expandableContainer-draggable">
              {path}
            </span>
            <div className="elyra-expandableContainer-action-buttons">
              <button
                title="Edit from editor"
                className="elyra-feedbackButton elyra-button elyra-expandableContainer-button elyra-expandableContainer-actionButton"
                onClick={() => {
                  this.props.commands.execute('docmanager:open', {
                    path: path
                  });
                }}
              >
                <span>
                  <svg
                    viewBox="0 0 1024 1024"
                    version="1.1"
                    xmlns="http://www.w3.org/2000/svg"
                    p-id="2987"
                    width="16"
                    height="16"
                  >
                    <path
                      d="M247.0912 996.1472 100.5568 996.1472c-39.936 0-72.4992-32.5632-72.4992-72.4992L28.0576 81.92c0-39.936 32.5632-72.4992 72.4992-72.4992l766.0544 0c39.936 0 72.4992 32.5632 72.4992 72.4992l0 210.5344c0 12.3904-10.0352 22.528-22.528 22.528s-22.528-10.0352-22.528-22.528L894.0544 81.92c0-15.1552-12.288-27.5456-27.5456-27.5456L100.5568 54.3744c-15.1552 0-27.5456 12.288-27.5456 27.5456L73.0112 923.648c0 15.1552 12.288 27.5456 27.5456 27.5456l146.5344 0c12.3904 0 22.528 10.0352 22.528 22.528S259.4816 996.1472 247.0912 996.1472z"
                      fill="#616161"
                      p-id="2988"
                    ></path>
                    <path
                      d="M745.2672 192.1024 174.6944 192.1024c-12.3904 0-22.528-10.0352-22.528-22.528s10.0352-22.528 22.528-22.528l570.5728 0c12.3904 0 22.528 10.0352 22.528 22.528S757.6576 192.1024 745.2672 192.1024z"
                      fill="#616161"
                      p-id="2989"
                    ></path>
                    <path
                      d="M437.6576 429.6704 174.6944 429.6704c-12.3904 0-22.528-10.0352-22.528-22.528s10.0352-22.528 22.528-22.528l262.9632 0c12.3904 0 22.528 10.0352 22.528 22.528S450.1504 429.6704 437.6576 429.6704z"
                      fill="#616161"
                      p-id="2990"
                    ></path>
                    <path
                      d="M620.6464 310.8864 174.6944 310.8864c-12.3904 0-22.528-10.0352-22.528-22.528s10.0352-22.528 22.528-22.528l445.952 0c12.3904 0 22.528 10.0352 22.528 22.528S633.1392 310.8864 620.6464 310.8864z"
                      fill="#616161"
                      p-id="2991"
                    ></path>
                    <path
                      d="M399.6672 1009.8688c-6.2464 0-12.288-2.56-16.5888-7.2704-5.2224-5.7344-7.168-13.7216-5.12-21.2992l40.8576-146.6368c1.024-3.6864 3.072-7.168 5.7344-9.8304l408.9856-408.9856c14.1312-14.0288 36.9664-14.0288 51.0976 0l97.792 97.792c6.8608 6.8608 10.5472 15.872 10.5472 25.4976s-3.7888 18.7392-10.5472 25.4976L928.8704 618.496c-4.1984 4.1984-9.9328 6.5536-15.872 6.5536s-11.6736-2.3552-15.872-6.5536l-66.048-66.048c-8.8064-8.8064-8.8064-23.04 0-31.8464s23.04-8.8064 31.8464 0l50.176 50.176 31.4368-31.4368L859.136 454.0416 460.6976 852.48 431.104 958.6688 546.7136 936.96l231.7312-231.7312c5.0176-5.4272 50.7904-52.6336 107.2128-56.7296 12.3904-0.9216 23.1424 8.3968 24.064 20.7872 0.9216 12.3904-8.3968 23.1424-20.7872 24.064-40.3456 2.9696-77.4144 42.2912-77.824 42.7008-0.2048 0.2048-0.4096 0.512-0.7168 0.7168L573.5424 973.7216c-3.1744 3.1744-7.2704 5.3248-11.776 6.2464l-158.0032 29.5936C402.432 1009.7664 401.1008 1009.8688 399.6672 1009.8688z"
                      fill="#616161"
                      p-id="2992"
                    ></path>
                  </svg>
                </span>
              </button>
              <button
                title="Edit from page"
                className="elyra-feedbackButton elyra-button elyra-expandableContainer-button elyra-expandableContainer-actionButton"
                onClick={() => {
                  this.openComponentEditor({
                    onSave: () => {},
                    schemaspace: 'component-catalogs',
                    schema: 'component-editor',
                    path: path
                  });
                }}
              >
                <span>
                  <svg
                    viewBox="0 0 1024 1024"
                    version="1.1"
                    xmlns="http://www.w3.org/2000/svg"
                    p-id="4238"
                    width="16"
                    height="16"
                  >
                    <path
                      d="M876.675 260.984L774.137 153.388 495.376 443.604l102.54 107.595zM413 639.438l164.068-68.143-101.68-106.718zM947.074 120.265l-41.01-43.033c-16.995-17.809-44.513-17.809-61.508 0l-51.291 53.798 102.539 107.594 51.27-53.774c16.995-17.832 16.995-46.751 0-64.585z"
                      fill="#616161"
                      p-id="4239"
                    ></path>
                    <path
                      d="M857.174 873.546c0 25.146-20.383 45.528-45.528 45.528H149.603c-25.145 0-45.528-20.383-45.528-45.528V212.637c0-25.145 20.383-45.528 45.528-45.528h491.521v-40H114.066c-27.61 0-49.992 22.382-49.992 49.992v731.982c0 27.611 22.382 49.992 49.992 49.992h733.116c27.611 0 49.992-22.381 49.992-49.992V383.79h-40v489.756z"
                      fill="#616161"
                      p-id="4240"
                    ></path>
                  </svg>
                </span>
              </button>
            </div>
          </div>
        </li>
      ));
    }

    return (
      <div>
        {/* <h6>Runtime Type</h6>
        {metadata.metadata.runtime_type}
        <br />
        <br />
        <h6>Description</h6>
        {metadata.metadata.description ?? 'No description'}
        <br />
        <br />
        <h6>Components</h6> */}
        <ul>{component_output}</ul>
      </div>
    );
  }

  // Allow for filtering by display_name, name, and description
  matchesSearch(searchValue: string, metadata: IMetadata): boolean {
    searchValue = searchValue.toLowerCase();
    // True if search string is in name or display_name,
    // or if the search string is empty
    const description = (metadata.metadata.description || '').toLowerCase();
    return (
      metadata.name.toLowerCase().includes(searchValue) ||
      metadata.display_name.toLowerCase().includes(searchValue) ||
      description.includes(searchValue)
    );
  }
}

/**
 * ComponentCatalogsWidget props.
 */
export interface IComponentCatalogsWidgetProps extends IMetadataWidgetProps {
  app: JupyterFrontEnd;
  themeManager?: IThemeManager;
  display_name: string;
  schemaspace: string;
  icon: LabIcon;
  titleContext?: string;
  appendToTitle?: boolean;
  refreshCallback?: () => void;
}

/**
 * A widget for displaying component catalogs.
 */
export class ComponentCatalogsWidget extends MetadataWidget {
  refreshButtonTooltip: string;
  refreshCallback?: () => void;
  commands?: any;

  constructor(props: IComponentCatalogsWidgetProps) {
    super(props);
    this.refreshCallback = props.refreshCallback;
    this.refreshButtonTooltip =
      'Refresh list and reload components from all catalogs';
    this.commands = props.app.commands;
  }

  // wrapper function that refreshes the palette after calling updateMetadata
  updateMetadataAndRefresh = (): void => {
    super.updateMetadata();
    if (this.refreshCallback) {
      this.refreshCallback();
    }
  };

  refreshMetadata(): void {
    PipelineService.refreshComponentsCache()
      .then((response: any): void => {
        this.updateMetadataAndRefresh();
      })
      .catch(error => handleError(error));
  }

  renderDisplay(metadata: IMetadata[]): React.ReactElement {
    if (Array.isArray(metadata) && !metadata.length) {
      // Empty metadata
      return (
        <div>
          <br />
          <h6 className="elyra-no-metadata-msg">
            Click the + button to add {this.props.display_name.toLowerCase()}
          </h6>
        </div>
      );
    }
    let filter_metadata = [];
    let hide_metadata = ['Actions', 'Events', 'Global', 'Logic'];
    for (let item in metadata) {
      if (hide_metadata.includes(metadata[item].display_name)) {
        continue;
      } else {
        filter_metadata.push(metadata[item]);
      }
    }
    return (
      <ComponentCatalogsDisplay
        metadata={filter_metadata}
        updateMetadata={this.updateMetadataAndRefresh}
        openMetadataEditor={this.openMetadataEditor}
        schemaspace={COMPONENT_CATALOGS_SCHEMASPACE}
        sortMetadata={true}
        className={COMPONENT_CATALOGS_CLASS}
        omitTags={this.omitTags()}
        titleContext={this.props.titleContext}
        commands={this.commands}
      />
    );
  }
}
