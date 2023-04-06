#
# Copyright 2018-2022 Elyra Authors
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

import yaml

from elyra.pipeline.catalog_connector import CatalogEntry
from elyra.pipeline.component import Component
from elyra.pipeline.component import ComponentParser
from elyra.pipeline.runtime_type import RuntimeProcessorType


class WfpComponentParser(ComponentParser):
    _file_types: List[str] = [".yaml"]

    component_platform: RuntimeProcessorType = RuntimeProcessorType.WORKFLOW_PIPELINES

    def parse(self, catalog_entry: CatalogEntry) -> Optional[List[Component]]:
        # Get YAML object from component definition
        component_yaml = self._read_component_yaml(catalog_entry)
        if not component_yaml:
            return None

        # Assign component_id and description
        description = ""
        if component_yaml.get("description"):
            # Remove whitespace characters and replace with spaces
            description = " ".join(component_yaml.get("description").split())

        component_properties = []

        component = catalog_entry.get_component(
            id=catalog_entry.id,
            name=component_yaml.get("name"),
            description=description,
            properties=component_properties,
            file_extension=self._file_types[0],
        )

        return [component]

    def _read_component_yaml(self, catalog_entry: CatalogEntry) -> Optional[Dict[str, Any]]:
        """
        Convert component_definition string to YAML object
        """
        try:
            results = yaml.safe_load(catalog_entry.entry_data.definition)
        except Exception as e:
            self.log.warning(
                f"Could not load YAML definition for component with identifying information: "
                f"'{catalog_entry.entry_reference}' -> {str(e)}"
            )
            return None

        return results
