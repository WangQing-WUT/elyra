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

from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional

from elyra.pipeline.catalog_connector import ComponentCatalogConnector
from elyra.pipeline.catalog_connector import EntryData
from elyra.pipeline.catalog_connector import WfpEntryData


class WorkFlowCatalogConnector(ComponentCatalogConnector):
    """
    Read component definitions from a workflow catalog
    """

    def get_catalog_entries(self, catalog_metadata: Dict[str, Any]) -> List[Dict[str, Any]]:

        """
        Returns a list of component_metadata instances, one per component found in the given registry.
        The form that component_metadata takes is determined by requirements of the reader class.

        :param registry_metadata: the dictionary-form of the Metadata instance for a single registry
        """
        component_list = []

        # Load user-provided catalog connector parameters. Required parameter
        # values (e.g. a catalog server URL) must be provided by the user
        # during connector creation. Optional parameters (e.g. a filter condition)
        # can be provided by the user during connector creation.
        # Parameters are defined in the 'workflow-catalog.json' schema file.

        workflow_required_parm = catalog_metadata["workflow_required_parm"]
        workflow_optional_parm = catalog_metadata.get("workflow_optional_parm")
        test_parm = catalog_metadata.get("test-parm")

        self.log.debug(f"Value of required parameter 'workflow_required_parm': {workflow_required_parm}")
        self.log.debug(f"Value of optional parameter 'workflow_optional_parm': {workflow_optional_parm}")
        self.log.debug(f"Value of optional parameter 'debug_optional_parm': {test_parm}")
        try:
            root_dir = Path(__file__).parent / "resources"
            pattern = "**/*.yaml"
            self.log.debug(f"Component file pattern: {pattern}")
            for file in root_dir.glob(pattern):
                component_list.append({"component-id": str(file)[len(str(root_dir)) + 1:]})
            self.log.debug(f"Component list: {component_list}")
        except Exception as ex:
            self.log.error(f"Error retrieving component list for runtime type '{ex}'")

        return component_list

    def get_entry_data(
        self, catalog_entry_data: Dict[str, Any], catalog_metadata: Dict[str, Any]
    ) -> Optional[EntryData]:
        """
        Fetch the definition that is identified by catalog_entry_data for the <TODO> catalog and
        create an EntryData object to represent it. If runtime-type-specific properties are required
        (e.g. `package_name` for certain Airflow operators), a runtime-type-specific EntryData
        object can be created, e.g. AirflowEntryData(definition=<...>, package_name=<...>)

        :param catalog_entry_data: a dictionary that contains the information needed to read the content
            of the component definition
        :param catalog_metadata: the metadata associated with the catalog in which this catalog entry is
            stored; in addition to catalog_entry_data, catalog_metadata may also be needed to read the
            component definition for certain types of catalogs

        :returns: An instance of EntryData, if the referenced catalog_entry_data was found
        """
        component_id = catalog_entry_data.get("component-id")
        if component_id is None:
            self.log.error("Cannot retrieve component: " "A component id must be provided.")
            return None
        try:
            # load component from resources directory
            root_dir = Path(__file__).parent / "resources"
            with open(root_dir / component_id, "r") as fp:
                return WfpEntryData(definition=fp.read())
        except Exception as e:
            self.log.error(f"Failed to fetch component '{component_id}' " f" from '{root_dir}': {str(e)}")
            return None

    @classmethod
    def get_hash_keys(cls) -> List[Any]:
        """
        Identifies the unique TODO catalog key that method get_entry_data
        can use to fetch a component from the catalog. Method get_catalog_entries
        retrieves the list of available key values from the catalog.

        :returns: a list of keys
        """
        return ["component-id"]
