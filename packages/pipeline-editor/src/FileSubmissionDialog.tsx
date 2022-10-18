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

import { IDictionary } from '@elyra/services';
import * as React from 'react';

import { IRuntimeData } from './runtime-utils';
import RuntimeConfigSelect from './RuntimeConfigSelect';
import Utils from './utils';

interface IProps {
  env: string[];
  dependencyFileExtension: string;
  images: IDictionary<string>;
  runtimeData: IRuntimeData;
}

const EnvForm: React.FC<{ env: string[] }> = ({ env }) => {
  if (env.length > 0) {
    return (
      <>
        <br />
        <br />
        <div>Environment Variables:</div>
        <br />
        {Utils.chunkArray(env, 4).map((col, i) => (
          <div key={i}>
            {col.map(envVar => (
              <div key={envVar}>
                <label htmlFor={envVar}>{envVar}:</label>
                <br />
                <input
                  type="text"
                  id={envVar}
                  className="envVar"
                  name={envVar}
                  size={30}
                />
              </div>
            ))}
          </div>
        ))}
      </>
    );
  }
  return null;
};

export const FileSubmissionDialog: React.FC<IProps> = ({
  env,
  images,
  dependencyFileExtension,
  runtimeData
}) => {
  const [includeDependency, setIncludeDependency] = React.useState(true);

  const handleToggle = (): void => {
    setIncludeDependency(prev => !prev);
  };

  return (
    <form className="elyra-dialog-form">
      <RuntimeConfigSelect runtimeData={runtimeData} />
      <label htmlFor="framework">Runtime Image:</label>
      <br />
      <select id="framework" name="framework" className="elyra-form-framework">
        {Object.entries(images).map(([key, val]) => (
          <option key={key} value={key}>
            {val}
          </option>
        ))}
      </select>
      <br />
      <div className="elyra-resourcesWrapper">
        <div className="elyra-resourceInput">
          <label htmlFor="cpu"> CPU:</label>
          <div className="elyra-resourceInputDescription" id="cpu-description">
            For CPU-intensive workloads, you can choose more than 1 CPU (e.g.
            1.5).
          </div>
          <input id="cpu" type="number" name="cpu" />
        </div>
        <div className="elyra-resourceInput">
          <label htmlFor="gpu"> GPU:</label>
          <div className="elyra-resourceInputDescription" id="gpu-description">
            For GPU-intensive workloads, you can choose more than 1 GPU. Must be
            an integer.
          </div>
          <input id="gpu" type="number" name="gpu" />
        </div>
        <div className="elyra-resourceInput">
          <label htmlFor="memory"> RAM (GB):</label>
          <div
            className="elyra-resourceInputDescription"
            id="memory-description"
          >
            The total amount of RAM specified.
          </div>
          <input id="memory" type="number" name="memory" />
        </div>
      </div>
      <br />
      <div className="elyra-resourcesWrapper">
        <div className="elyra-resourceInput">
          <label htmlFor="npu310"> NPU310:</label>
          <div
            className="elyra-resourceInputDescription"
            id="npu310-description"
          >
            For NPU310-intensive workloads, you can choose more than 1 NPU310
            (e.g. 1.5).
          </div>
          <input id="npu310" type="number" name="npu310" />
        </div>
        <div className="elyra-resourceInput">
          <label htmlFor="npu910"> NPU910:</label>
          <div
            className="elyra-resourceInputDescription"
            id="npu910-description"
          >
            For NPU910-intensive workloads, you can choose more than 1 NPU910.
            Must be an integer.
          </div>
          <input id="npu910" type="number" name="npu910" />
        </div>
      </div>
      <br />
      <input
        type="checkbox"
        className="elyra-Dialog-checkbox"
        id="dependency_include"
        name="dependency_include"
        size={20}
        checked={includeDependency}
        onChange={handleToggle}
      />
      <label htmlFor="dependency_include">Include File Dependencies:</label>
      <br />
      {includeDependency && (
        <div key="dependencies">
          <br />
          <input
            type="text"
            id="dependencies"
            className="jp-mod-styled"
            name="dependencies"
            placeholder={`*${dependencyFileExtension}`}
            defaultValue={`*${dependencyFileExtension}`}
            size={30}
          />
        </div>
      )}
      <EnvForm env={env} />
    </form>
  );
};
