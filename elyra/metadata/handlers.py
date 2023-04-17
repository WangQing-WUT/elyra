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
import os

from jsonschema import ValidationError
from jupyter_server.base.handlers import APIHandler
from jupyter_server.utils import url_path_join
from jupyter_server.utils import url_unescape
from tornado import web
import yaml

from elyra.metadata.error import MetadataExistsError
from elyra.metadata.error import MetadataNotFoundError
from elyra.metadata.error import SchemaNotFoundError
from elyra.metadata.manager import MetadataManager
from elyra.metadata.metadata import Metadata
from elyra.metadata.schema import SchemaManager
from elyra.util.http import HttpErrorMixin


class MetadataHandler(HttpErrorMixin, APIHandler):
    """Handler for metadata configurations collection."""

    @web.authenticated
    async def get(self, schemaspace):
        schemaspace = url_unescape(schemaspace)
        parent = self.settings.get("elyra")
        try:
            metadata_manager = MetadataManager(schemaspace=schemaspace, parent=parent)
            metadata = metadata_manager.get_all()
        except (ValidationError, ValueError) as err:
            raise web.HTTPError(400, str(err)) from err
        except MetadataNotFoundError as err:
            raise web.HTTPError(404, str(err)) from err
        except Exception as err:
            raise web.HTTPError(500, repr(err)) from err

        metadata_model = {schemaspace: [r.to_dict(trim=True) for r in metadata]}
        self.set_header("Content-Type", "application/json")
        self.finish(metadata_model)

    @web.authenticated
    async def post(self, schemaspace):

        schemaspace = url_unescape(schemaspace)
        parent = self.settings.get("elyra")
        server_root_dir = self.settings.get("server_root_dir")
        try:
            instance = self._validate_body(schemaspace)
            instance_dict = instance.to_dict()
            if instance_dict.get("schema_name") == "new-component":
                if "save_path" not in instance_dict.get("metadata"):
                    instance_dict["metadata"]["save_path"] = server_root_dir
                else:
                    if "." not in instance_dict.get("metadata").get("save_path"):
                        instance_dict["metadata"]["save_path"] += "/"
                    dirname = os.path.dirname(instance_dict.get("metadata").get("save_path"))
                    instance_dict["metadata"]["save_path"] = os.path.join(server_root_dir, dirname)
                if "file_name" not in instance_dict.get("metadata"):
                    instance_dict["metadata"]["file_name"] = instance_dict.get("metadata").get("component_name")
                instance_dict["metadata"]["root_dir"] = server_root_dir
                instance = Metadata.from_dict(schemaspace, instance_dict)
            elif instance_dict["schema_name"] == "local-file-catalog":
                instance_dict["metadata"]["base_path"] = server_root_dir
                instance = Metadata.from_dict(schemaspace, instance_dict)

            self.log.debug(
                f"MetadataHandler: Creating metadata instance '{instance.name}' in schemaspace '{schemaspace}'..."
            )
            metadata_manager = MetadataManager(schemaspace=schemaspace, parent=parent)
            metadata = metadata_manager.create(instance.name, instance)
        except (ValidationError, ValueError, SyntaxError) as err:
            raise web.HTTPError(400, str(err)) from err
        except (MetadataNotFoundError, SchemaNotFoundError) as err:
            raise web.HTTPError(404, str(err)) from err
        except MetadataExistsError as err:
            raise web.HTTPError(409, str(err)) from err
        except Exception as err:
            raise web.HTTPError(500, repr(err)) from err

        self.set_status(201)
        self.set_header("Content-Type", "application/json")
        location = url_path_join(self.base_url, "elyra", "metadata", schemaspace, metadata.name)
        self.set_header("Location", location)
        self.finish(metadata.to_dict(trim=True))

    def _validate_body(self, schemaspace: str):
        """Validates the body issued for creates."""
        body = self.get_json_body()

        # Ensure schema_name and metadata fields exist.
        required_fields = ["schema_name", "metadata"]
        for field in required_fields:
            if field not in body:
                raise SyntaxError(f"Insufficient information - '{field}' is missing from request body.")

        # Ensure there is at least one of name or a display_name
        one_of_fields = ["name", "display_name"]
        if set(body).isdisjoint(one_of_fields):
            raise SyntaxError(
                f"Insufficient information - request body requires one of the following: {one_of_fields}."
            )

        instance = Metadata.from_dict(schemaspace, {**body})
        return instance


class MetadataResourceHandler(HttpErrorMixin, APIHandler):
    """Handler for metadata configuration specific resource (e.g. a runtime element)."""

    @web.authenticated
    async def get(self, schemaspace, resource):
        schemaspace = url_unescape(schemaspace)
        resource = url_unescape(resource)
        parent = self.settings.get("elyra")

        try:
            metadata_manager = MetadataManager(schemaspace=schemaspace, parent=parent)
            metadata = metadata_manager.get(resource)
        except (ValidationError, ValueError, NotImplementedError) as err:
            raise web.HTTPError(400, str(err)) from err
        except MetadataNotFoundError as err:
            raise web.HTTPError(404, str(err)) from err
        except Exception as err:
            raise web.HTTPError(500, repr(err)) from err

        self.set_header("Content-Type", "application/json")
        self.finish(metadata.to_dict(trim=True))

    @web.authenticated
    async def put(self, schemaspace, resource):
        schemaspace = url_unescape(schemaspace)
        resource = url_unescape(resource)
        parent = self.settings.get("elyra")

        try:
            payload = self.get_json_body()
            # Get the current resource to ensure its pre-existence
            metadata_manager = MetadataManager(schemaspace=schemaspace, parent=parent)
            metadata_manager.get(resource)
            payload["metadata"]["base_path"] = self.settings.get("server_root_dir")
            # Check if name is in the payload and varies from resource, if so, raise 400
            if "name" in payload and payload["name"] != resource:
                raise NotImplementedError(
                    f"The attempt to rename instance '{resource}' to '{payload['name']}' is not supported."
                )
            instance = Metadata.from_dict(schemaspace, {**payload})
            self.log.debug(
                f"MetadataHandler: Updating metadata instance '{resource}' in schemaspace '{schemaspace}'..."
            )
            metadata = metadata_manager.update(resource, instance)
        except (ValidationError, ValueError, NotImplementedError) as err:
            raise web.HTTPError(400, str(err)) from err
        except MetadataNotFoundError as err:
            raise web.HTTPError(404, str(err)) from err
        except Exception as err:
            raise web.HTTPError(500, repr(err)) from err

        self.set_status(200)
        self.set_header("Content-Type", "application/json")
        self.finish(metadata.to_dict(trim=True))

    @web.authenticated
    async def delete(self, schemaspace, resource):
        schemaspace = url_unescape(schemaspace)
        resource = url_unescape(resource)
        parent = self.settings.get("elyra")

        try:
            self.log.debug(
                f"MetadataHandler: Deleting metadata instance '{resource}' in schemaspace '{schemaspace}'..."
            )
            metadata_manager = MetadataManager(schemaspace=schemaspace, parent=parent)
            metadata_manager.remove(resource)
        except (ValidationError, ValueError) as err:
            raise web.HTTPError(400, str(err)) from err
        except PermissionError as err:
            raise web.HTTPError(403, str(err)) from err
        except MetadataNotFoundError as err:
            raise web.HTTPError(404, str(err)) from err
        except Exception as err:
            raise web.HTTPError(500, repr(err)) from err

        self.set_status(204)
        self.finish()


class ComponentEditorHandler(HttpErrorMixin, APIHandler):
    """Handler for component editor."""

    @web.authenticated
    async def put(self, path):
        path = url_unescape(path)
        parent = self.settings.get("elyra")
        payload = self.get_json_body()
        try:
            metadata_manager = MetadataManager(schemaspace="component-catalogs", parent=parent)
            metadata_manager.save_component(payload.get("metadata"), path)
        except (ValidationError, ValueError, NotImplementedError) as err:
            raise web.HTTPError(400, str(err)) from err
        except MetadataNotFoundError as err:
            raise web.HTTPError(404, str(err)) from err
        except Exception as err:
            raise web.HTTPError(500, err) from err

        self.set_status(200)
        self.set_header("Content-Type", "application/json")
        self.finish(payload)

    @web.authenticated
    async def get(self, path):
        path = url_unescape(path)
        component_metadata = {"schema_name": "component-editor", "metadata": {}}
        component_absolute_path = os.path.join(os.getcwd(), path)

        try:
            with open(component_absolute_path, "r", encoding="utf-8") as r:
                component_yaml = yaml.safe_load(r.read())
                metadata = component_metadata.get("metadata")
                input_parameters_placeholder = {}
                if "name" in component_yaml:
                    metadata["component_name"] = component_yaml.get("name")
                if "description" in component_yaml:
                    metadata["component_description"] = component_yaml.get("description")
                if "implementation" in component_yaml:
                    if "container" in component_yaml.get("implementation"):
                        container = component_yaml.get("implementation").get("container")
                        metadata["implementation"] = {}
                        if "image" in container:
                            metadata["implementation"]["image_name"] = container.get("image")
                        if "command" in container:
                            command_str = ""
                            for item in container.get("command"):
                                if "\n" in item:
                                    command_str += "- |\n  " + item.replace("\n", "\n  ").rstrip() + "\n"
                                else:
                                    command_str += "- " + str(item) + "\n"
                                if type(item) is dict:
                                    for i in item:
                                        input_parameters_placeholder[item[i]] = i
                            metadata["implementation"]["command"] = command_str
                        if "args" in container:
                            args_str = ""
                            for item in container.get("args"):
                                args_str += "- {}\n".format(item)
                            metadata["implementation"]["args"] = args_str
                if "inputs" in component_yaml:
                    metadata["input_parameters"] = []
                    for input_parameter in component_yaml.get("inputs"):
                        temp_input_parameter = {}
                        if "name" in input_parameter:
                            temp_input_parameter["name"] = input_parameter.get("name")
                            if input_parameter["name"] in input_parameters_placeholder:
                                temp_input_parameter["placeholder_type"] = input_parameters_placeholder[
                                    input_parameter.get("name")
                                ]
                            else:
                                temp_input_parameter["placeholder_type"] = "inputValue"
                        if "type" in input_parameter:
                            temp_input_parameter["value_type"] = input_parameter.get("type")
                        if "default" in input_parameter:
                            temp_input_parameter["default"] = input_parameter.get("default")
                        if "description" in input_parameter:
                            temp_input_parameter["description"] = input_parameter.get("description")
                        metadata["input_parameters"].append(temp_input_parameter)
                if "outputs" in component_yaml:
                    metadata["output_parameters"] = []
                    for output_parameter in component_yaml.get("outputs"):
                        temp_output_parameter = {}
                        if "name" in output_parameter:
                            temp_output_parameter["name"] = output_parameter.get("name")
                        if "type" in output_parameter:
                            temp_output_parameter["value_type"] = output_parameter.get("type")
                        else:
                            temp_output_parameter["value_type"] = "String"
                        if "description" in output_parameter:
                            temp_output_parameter["description"] = output_parameter.get("description")
                        temp_output_parameter["placeholder_type"] = "outputPath"
                        metadata.get("output_parameters").append(temp_output_parameter)
        except (ValidationError, ValueError, SchemaNotFoundError) as err:
            raise web.HTTPError(404, str(err)) from err
        except Exception as err:
            raise web.HTTPError(500, str(err)) from err

        self.set_header("Content-Type", "application/json")
        self.finish(component_metadata)


class SchemaHandler(HttpErrorMixin, APIHandler):
    """Handler for schemaspace schemas."""

    @web.authenticated
    async def get(self, schemaspace):
        schemaspace = url_unescape(schemaspace)

        try:
            schema_manager = SchemaManager.instance()
            schemas = schema_manager.get_schemaspace_schemas(schemaspace)
        except (ValidationError, ValueError, SchemaNotFoundError) as err:
            raise web.HTTPError(404, str(err)) from err
        except Exception as err:
            raise web.HTTPError(500, repr(err)) from err

        schemas_model = {schemaspace: list(schemas.values())}
        self.set_header("Content-Type", "application/json")
        self.finish(schemas_model)


class SchemaResourceHandler(HttpErrorMixin, APIHandler):
    """Handler for a specific schema (resource) for a given schemaspace."""

    @web.authenticated
    async def get(self, schemaspace, resource):
        schemaspace = url_unescape(schemaspace)
        resource = url_unescape(resource)

        try:
            schema_manager = SchemaManager.instance()
            schema = schema_manager.get_schema(schemaspace, resource)
        except (ValidationError, ValueError, SchemaNotFoundError) as err:
            raise web.HTTPError(404, str(err)) from err
        except Exception as err:
            raise web.HTTPError(500, repr(err)) from err

        self.set_header("Content-Type", "application/json")
        self.finish(schema)


class SchemaspaceHandler(HttpErrorMixin, APIHandler):
    """Handler for retrieving schemaspace names."""

    @web.authenticated
    async def get(self):

        try:
            schema_manager = SchemaManager.instance()
            schemaspaces = schema_manager.get_schemaspace_names()
        except (ValidationError, ValueError) as err:
            raise web.HTTPError(404, str(err)) from err
        except Exception as err:
            raise web.HTTPError(500, repr(err)) from err

        schemaspace_model = {"schemaspaces": schemaspaces}

        self.set_header("Content-Type", "application/json")
        self.finish(schemaspace_model)


class SchemaspaceResourceHandler(HttpErrorMixin, APIHandler):
    """Handler for retrieving schemaspace JSON info (id, display name and descripton) for a given schemaspace."""

    @web.authenticated
    async def get(self, schemaspace):

        try:
            schema_manager = SchemaManager.instance()
            schemaspace = schema_manager.get_schemaspace(schemaspace)

        except (ValidationError, ValueError) as err:
            raise web.HTTPError(404, str(err)) from err
        except Exception as err:
            raise web.HTTPError(500, repr(err)) from err

        schemaspace_info_model = {
            "name": schemaspace.name,
            "id": schemaspace.id,
            "display_name": schemaspace.display_name,
            "description": schemaspace.description,
        }

        self.set_header("Content-Type", "application/json")
        self.finish(schemaspace_info_model)
