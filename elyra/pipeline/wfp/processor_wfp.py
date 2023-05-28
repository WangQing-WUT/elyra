import json
import os
from pathlib import Path
import re
import zipfile

import requests
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import PreservedScalarString as pss
import yaml

from elyra.metadata.schemaspaces import Runtimes
from elyra.pipeline.parser import PipelineParser
from elyra.pipeline.processor import PipelineProcessorManager
from elyra.pipeline.processor import RuntimePipelineProcessor
from elyra.pipeline.processor import RuntimePipelineProcessorResponse
from elyra.pipeline.runtime_type import RuntimeProcessorType
from elyra.pipeline.validation import PipelineValidationManager
from elyra.pipeline.validation import ValidationResponse
from elyra.pipeline.validation import ValidationSeverity


class WfpPipelineProcessor(RuntimePipelineProcessor):

    _type = RuntimeProcessorType.WORKFLOW_PIPELINES
    _name = "wfp"

    @staticmethod
    def create_pipeline_template(path: str, resource: str, file_list):
        yaml_loader = YAML()
        str_yaml = pss(resource)
        name = Path(str(path)).stem
        pipeline_template = {
            "apiVersion": "wfe.hiascend.com/v1",
            "kind": "PipelineTemplate",
            "metadata": {"name": name, "namespace": "kubeflow"},
            "spec": {"resource": str_yaml},
        }
        save_path = path.replace(".yaml", "-pipeline.yaml")
        file_fd = os.open(save_path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o666)
        with os.fdopen(file_fd, "w") as file:
            yaml_loader.dump(pipeline_template, file)
        file_list.append(save_path)

    async def export_pipeline(self, path, runtime_config, root_dir, parent, file_list):
        response = ValidationResponse()
        pipeline_input_parameters = []
        resource = ""
        if path.endswith(".pipeline"):
            pipeline_file = open(path, "r", encoding="UTF-8")
            pipeline_definition = json.load(pipeline_file)
            pipeline_definition["pipelines"][0]["app_data"]["runtime_config"] = runtime_config
            pipeline_definition["pipelines"][0]["app_data"]["runtime"] = "kfp"
            pipeline_definition["pipelines"][0]["app_data"]["name"] = Path(str(path)).stem
            pipeline_definition["pipelines"][0]["app_data"]["source"] = path

            response = await PipelineValidationManager.instance().validate(pipeline_definition)

            if not response.has_fatal:
                pipeline = PipelineParser(root_dir=root_dir, parent=parent).parse(pipeline_definition)
                await PipelineProcessorManager.instance().export(
                    pipeline, "yaml", path.replace(".pipeline", ".yaml"), True
                )
                pipeline_yaml = open(path.replace(".pipeline", ".yaml"), "r", encoding="utf-8")
                resource = pipeline_yaml.read()
                self.create_pipeline_template(path.replace(".pipeline", ".yaml"), resource, file_list)
        elif path.endswith(".yaml"):
            pipeline_yaml = open(path, "r", encoding="utf-8")
            resource = pipeline_yaml.read()
            self.create_pipeline_template(path, resource, file_list)
        if not response.has_fatal:
            export_pipeline_yaml = yaml.safe_load(resource)
            if export_pipeline_yaml.get("metadata", {}).get("annotations", {}):
                pipeline_spec_dict = json.loads(
                    export_pipeline_yaml.get("metadata").get("annotations").get("pipelines.kubeflow.org/pipeline_spec")
                )
                for item in pipeline_spec_dict.get("inputs", []):
                    pipeline_input_parameter = {}
                    pipeline_input_parameter["name"] = item.get("name")
                    pipeline_input_parameter["value"] = item.get("default")
                    pipeline_input_parameters.append(pipeline_input_parameter)
            else:
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="The yaml file is not an exported file for the pipeline.",
                    runtime="WORKFLOW",
                )

        return response, pipeline_input_parameters

    @staticmethod
    def _sepc_parse(
        init: dict,
        exit_parameters: dict,
        parameters: list,
        events: dict,
        triggers: dict,
    ):
        spec_field = {}
        if init and init.get("pipeline"):
            spec_field["init"] = init
        if exit_parameters and exit_parameters.get("pipeline"):
            spec_field["exit"] = exit_parameters
        if events:
            spec_field["events"] = events
        if triggers:
            spec_field["triggers"] = triggers
        spec_field["parameters"] = parameters
        return spec_field

    def _get_dataset_or_model_names(self, names: list):
        names_list = []
        for item in names:
            value = self._widget_value_str(item)
            if value:
                names_list.append(value)
        return names_list

    def _get_model_monitor_event_filter(self, event_filter: list):
        result = []
        for item in event_filter:
            temp_filter = {}
            temp_filter["AppName"] = self._widget_value_str(item.get("app_name"))
            temp_filter["ModelName"] = self._widget_value_str(item.get("model_name"))
            result.append(temp_filter)
        return result

    def _parse_model_event(self, event_field, node):
        if "model" not in event_field:
            event_field["model"] = {}

        event_field["model"][node.get("app_data").get("label").strip()] = {
            "models": self._get_dataset_or_model_names(
                node.get("app_data").get("component_parameters").get("model_name")
            ),
            "eventFilter": {
                "expression": self._get_event_filter(
                    node.get("app_data").get("component_parameters").get("event_filter"),
                    node.get("app_data").get("component_parameters").get("expression"),
                )
            },
        }

    def _parse_s3_event(self, event_field, node):
        if "s3" not in event_field:
            event_field["s3"] = {}

        event_field["s3"][node.get("app_data").get("label").strip()] = {
            "bucket": {
                "name": self._widget_value_str(node.get("app_data").get("component_parameters").get("bucket_name"))
            },
            "eventFilter": {"expression": self._get_s3_event_filter(node.get("app_data").get("component_parameters"))},
        }

    def _parse_calendar_event(self, event_field, node):
        if "calendar" not in event_field:
            event_field["calendar"] = {}

        calendar = node.get("app_data").get("component_parameters").get("calendar")
        event_field["calendar"][node.get("app_data").get("label").strip()] = {
            calendar.get("name"): self._widget_value_str(calendar.get("value"))
        }

    def _parse_dataset_event(self, event_field, node):
        if "dataset" not in event_field:
            event_field["dataset"] = {}

        event_field["dataset"][node.get("app_data").get("label").strip()] = {
            "datasets": self._get_dataset_or_model_names(
                node.get("app_data").get("component_parameters").get("dataset_name")
            ),
            "eventFilter": {
                "expression": self._get_event_filter(
                    node.get("app_data").get("component_parameters").get("event_filter"),
                    node.get("app_data").get("component_parameters").get("expression"),
                )
            },
        }

    def _parse_model_monitor_event(self, event_field, node):
        if "modelMonitor" not in event_field:
            event_field["modelMonitor"] = {}

        event_field["modelMonitor"][node.get("app_data").get("label").strip()] = {
            "alertName": self._widget_value_str(node.get("app_data").get("component_parameters").get("alert_name")),
            "eventFilter": self._get_model_monitor_event_filter(
                node.get("app_data").get("component_parameters").get("event_filter")
            ),
        }

    def _parse_k8s_object_trigger(self, trigger_field, node_json, node, response):
        if "k8sobj" not in trigger_field:
            trigger_field["k8sobj"] = {}

        node_type = "K8s Object Trigger"
        condition, linknodes_name_to_type = self._get_condition(node_json, node)
        parameters = self._parse_trigger_parameters(
            node,
            node_type,
            linknodes_name_to_type,
            response,
        )
        trigger_field["k8sobj"][node.get("app_data").get("label").strip()] = {
            "condition": condition,
            "operation": node.get("app_data").get("component_parameters").get("operation"),
            "source": self._get_k8s_source(node.get("app_data").get("component_parameters").get("source")),
            "arguments": parameters,
        }

    def _parse_http_trigger(self, trigger_field, node_json, node, response):
        if "http" not in trigger_field:
            trigger_field["http"] = {}

        node_type = "HTTP Trigger"
        condition, linknodes_name_to_type = self._get_condition(node_json, node)
        parameters = self._parse_trigger_parameters(
            node,
            node_type,
            linknodes_name_to_type,
            response,
        )

        trigger_field["http"][node.get("app_data").get("label").strip()] = {
            "condition": condition,
            "url": node.get("app_data").get("component_parameters").get("url"),
            "method": node.get("app_data").get("component_parameters").get("method"),
            "timeout": node.get("app_data").get("component_parameters").get("timeout"),
            "payload": parameters,
        }

    async def _parse_pipeline_trigger(
        self, trigger_field, node_json, node, runtime_config, root_dir, parent, file_list, response
    ):
        if "pipeline" not in trigger_field:
            trigger_field["pipeline"] = {}

        node_type = "Pipeline Trigger"
        condition, linknodes_name_to_type = self._get_condition(node_json, node)
        parameters = self._parse_trigger_parameters(
            node,
            node_type,
            linknodes_name_to_type,
            response,
        )
        path = node.get("app_data").get("component_parameters").get("template_name")
        (
            pipeline_response,
            pipeline_input_parameters,
        ) = await self.export_pipeline(path, runtime_config, root_dir, parent, file_list)
        if pipeline_response.has_fatal:
            issues = (pipeline_response.response).get("issues")
            node_id = node.get("id")
            node_name = node.get("app_data").get("label").strip()
            for issue in issues:
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="pipelineExportError",
                    message="Pipeline Export Error",
                    runtime="WORKFLOW",
                    data={
                        "nodeType": node_type,
                        "nodeID": node_id,
                        "nodeName": node_name,
                        "issue": issue,
                    },
                )
            return
        for parameter in parameters:
            for pipeline_input_parameter in pipeline_input_parameters:
                if pipeline_input_parameter.get("name") == parameter.get("name"):
                    pipeline_input_parameters.remove(pipeline_input_parameter)
                    break
        parameters += pipeline_input_parameters
        condition, _ = self._get_condition(node_json, node)
        trigger_field["pipeline"][node.get("app_data").get("label").strip()] = {
            "condition": condition,
            "operation": "create",
            "pipeline": {
                "pipelineTemplate": Path(
                    str(node.get("app_data").get("component_parameters").get("template_name"))
                ).stem,
                "parameters": parameters,
            },
        }

    async def _parse_init(self, node, runtime_config, root_dir, parent, file_list, response):
        init_field = {}
        init_pipeline_field = {}
        pipeline_input_parameters = {}
        pipeline_path = node.get("app_data").get("component_parameters").get("init_pipeline")
        path_name = Path(str(pipeline_path)).stem
        init_pipeline_field["pipelineTemplate"] = path_name
        (
            pipeline_response,
            pipeline_input_parameters,
        ) = await self.export_pipeline(pipeline_path, runtime_config, root_dir, parent, file_list)
        if pipeline_response.has_fatal:
            issues = (pipeline_response.response).get("issues")
            for issue in issues:
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="pipelineExportError",
                    message="Pipeline Export Error",
                    runtime="WORKFLOW",
                    data={
                        "nodeType": "Init",
                        "nodeID": node.get("id"),
                        "issue": issue,
                    },
                )
            return init_field
        init_parameters = []
        for init_item in node.get("app_data").get("component_parameters").get("init_parameters"):
            temp_value = self._widget_value_str(init_item.get("value"))
            temp_init = {
                "name": init_item.get("name"),
                "value": temp_value,
            }
            if "description" in init_item:
                temp_init["description"] = init_item.get("description")
            init_parameters.append(temp_init)
        for init_parameter in init_parameters:
            for pipeline_input_parameter in pipeline_input_parameters:
                if pipeline_input_parameter.get("name") == init_parameter.get("name"):
                    pipeline_input_parameters.remove(pipeline_input_parameter)
                    break
        init_parameters += pipeline_input_parameters
        init_pipeline_field["parameters"] = init_parameters
        init_pipeline_field = dict(sorted(init_pipeline_field.items(), key=lambda x: x[0], reverse=True))
        init_field = {"pipeline": init_pipeline_field}
        return init_field

    async def _parse_exit(self, node, runtime_config, root_dir, parent, file_list, response):
        exit_field = {}
        exit_pipeline_field = {}
        pipeline_input_parameters = {}
        pipeline_path = node.get("app_data").get("component_parameters").get("exit_pipeline")
        path_name = Path(str(pipeline_path)).stem
        exit_pipeline_field["pipelineTemplate"] = path_name
        (
            pipeline_response,
            pipeline_input_parameters,
        ) = await self.export_pipeline(pipeline_path, runtime_config, root_dir, parent, file_list)
        if pipeline_response.has_fatal:
            issues = (pipeline_response.response).get("issues")
            for issue in issues:
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="pipelineExportError",
                    message="Pipeline Export Error",
                    runtime="WORKFLOW",
                    data={
                        "nodeType": "Exit",
                        "nodeID": node.get("id"),
                        "issue": issue,
                    },
                )
            return exit_field
        exit_parameters = []
        for exit_item in node.get("app_data").get("component_parameters").get("exit_parameters"):
            temp_value = self._widget_value_str(exit_item.get("value"))
            temp_exit = {
                "name": exit_item.get("name"),
                "value": temp_value,
            }
            if "description" in exit_item:
                temp_exit["description"] = exit_item.get("description")
            exit_parameters.append(temp_exit)
        for exit_parameter in exit_parameters:
            for pipeline_input_parameter in pipeline_input_parameters:
                if pipeline_input_parameter.get("name") == exit_parameter.get("name"):
                    pipeline_input_parameters.remove(pipeline_input_parameter)
                    break
        exit_parameters += pipeline_input_parameters
        exit_pipeline_field["parameters"] = exit_parameters
        exit_pipeline_field = dict(
            sorted(
                exit_pipeline_field.items(),
                key=lambda x: x[0],
                reverse=True,
            )
        )
        exit_field = {"pipeline": exit_pipeline_field}
        return exit_field

    async def _component_parse(
        self,
        root_dir,
        parent,
        node_json: dict,
        runtime_config,
        file_list,
    ):
        response = ValidationResponse(runtime="WORKFLOW")
        init_field = {}
        exit_field = {}
        event_field = {}
        trigger_field = {}
        parser = {
            "model_event": self._parse_model_event,
            "s3_event": self._parse_s3_event,
            "calendar_event": self._parse_calendar_event,
            "dataset_event": self._parse_dataset_event,
            "model_monitor_event": self._parse_model_monitor_event,
            "pipeline_trigger": self._parse_pipeline_trigger,
            "k8s_object_trigger": self._parse_k8s_object_trigger,
            "http_trigger": self._parse_http_trigger,
            "init": self._parse_init,
            "exit": self._parse_exit,
        }
        for node in node_json:
            node_type = node.get("op").split(":")[0]
            parse_func = parser[node_type]
            if node_type in ["init", "exit", "pipeline_trigger"]:
                if node_type == "init":
                    init_field = await parse_func(node, runtime_config, root_dir, parent, file_list, response)
                elif node_type == "exit":
                    exit_field = await parse_func(node, runtime_config, root_dir, parent, file_list, response)
                elif node_type == "pipeline_trigger":
                    await parse_func(
                        trigger_field, node_json, node, runtime_config, root_dir, parent, file_list, response
                    )
                if response.has_fatal:
                    return [init_field, event_field, trigger_field, exit_field, response]
            elif node_type in ["k8s_object_trigger", "http_trigger"]:
                parse_func(trigger_field, node_json, node, response)
            else:
                parse_func(event_field, node)
        return [init_field, event_field, trigger_field, exit_field, response]

    def _get_condition(self, node_json: dict, node: dict):
        field_map = {
            "model_event": "model",
            "s3_event": "s3",
            "calendar_event": "calendar",
            "dataset_event": "dataset",
            "model_monitor_event": "modelMonitor",
            "pipeline_trigger": "pipeline",
            "k8s_object_trigger": "k8sobj",
            "http_trigger": "http",
        }
        condition = ""
        condition_num = 0
        linknodes_name_to_type = {}
        for link in node.get("inputs")[0].get("links"):
            node_type = self._get_type(node_json, link.get("node_id_ref"))
            node_name = self._get_name(node_json, link.get("node_id_ref"))
            linknodes_name_to_type[node_name] = field_map[node_type]
            if condition_num == 0:
                condition = "events." + field_map[node_type] + "." + node_name
                condition_num += 1
            elif condition_num == 1:
                condition = "(" + condition + ")" + " && (events." + field_map[node_type] + "." + node_name + ")"
                condition_num += 1
            else:
                condition += " && (events." + field_map[node_type] + "." + node_name + ")"

        return condition, linknodes_name_to_type

    def _get_event_filter(self, event_filter: list[dict], expression: str):
        expression = expression.replace(" ", "")
        stack_expression = ""
        expression_id = 0
        for char in expression:
            if char.isdigit():
                expression_id = 10 * expression_id + int(char)
            else:
                expression_item = ""
                if expression_id in range(1, len(event_filter) + 1):
                    expression_item = self._get_expression_item(expression_id, event_filter)
                if expression_item == "":
                    stack_expression += char
                else:
                    stack_expression += "(" + expression_item + ")" + char
                expression_id = 0
        if expression_id != 0:
            expression_item = self._get_expression_item(expression_id, event_filter)
            if stack_expression == "":
                stack_expression += expression_item
            else:
                stack_expression += "(" + expression_item + ")"
        stack_expression = stack_expression.replace("&&", " && ")
        stack_expression = stack_expression.replace("||", " || ")
        return stack_expression

    def _widget_value_str(self, value_dict: dict):
        value = value_dict.get("value").strip()
        if value == "":
            return ""
        elif value_dict.get("widget") == "string":
            return value
        elif value_dict.get("widget") == "enum":
            return "{{" + value + "}}"
        else:
            return ""

    def _get_s3_event_filter(self, component_parameters: dict):
        expression = ""
        prefix = self._widget_value_str(component_parameters.get("prefix"))
        suffix = self._widget_value_str(component_parameters.get("suffix"))
        event_filter = component_parameters.get("event_filter")
        if prefix:
            expression += "object prefix '" + prefix + "'"
        if suffix:
            if expression == "":
                expression += "object suffix '" + suffix + "'"
            else:
                expression = "(" + expression + ") && " + "(object suffix '" + suffix + "')"
        filter_expression = ""
        for item in event_filter:
            value = self._widget_value_str(item.get("value"))
            if filter_expression == "":
                filter_expression = item.get("name") + " " + item.get("operate") + " '" + value + "'"
            else:
                filter_expression = (
                    "("
                    + filter_expression
                    + ") || ("
                    + item.get("name")
                    + " "
                    + item.get("operate")
                    + " '"
                    + value
                    + "')"
                )
        if "&&" not in expression and expression:
            expression = "(" + expression + ")"
        if expression == "":
            expression = filter_expression
        else:
            expression = expression + " && (" + filter_expression + ")"

        return expression

    @staticmethod
    def _get_expression_item(expression_id: int, event_filter: list):
        expression_item = ""
        if expression_id in range(1, len(event_filter) + 1):
            expression_item = event_filter[expression_id - 1].get("name") + " "
            if type(event_filter[expression_id - 1].get("value")) == str:
                expression_item += (
                    event_filter[expression_id - 1].get("operate")
                    + " '"
                    + str(event_filter[expression_id - 1].get("value"))
                    + "'"
                )
            else:
                if type(event_filter[expression_id - 1].get("value").get("value")) == int:
                    expression_item += (
                        event_filter[expression_id - 1].get("operate")
                        + " "
                        + str(event_filter[expression_id - 1].get("value").get("value"))
                    )
                elif type(event_filter[expression_id - 1].get("value").get("value")) == str:
                    if event_filter[expression_id - 1].get("value").get("widget") == "string":
                        expression_item += (
                            event_filter[expression_id - 1].get("operate")
                            + " '"
                            + event_filter[expression_id - 1].get("value").get("value")
                            + "'"
                        )
                    elif event_filter[expression_id - 1].get("value").get("widget") == "enum":
                        expression_item += (
                            event_filter[expression_id - 1].get("operate")
                            + " '{{"
                            + event_filter[expression_id - 1].get("value").get("value")
                            + "}}'"
                        )
        return expression_item

    @staticmethod
    def _get_type(node_json: dict, node_id: str):
        for node in node_json:
            if node.get("id") == node_id:
                return node.get("op").split(":")[0]
            else:
                continue

    @staticmethod
    def _get_name(node_json: dict, node_id: str):
        for node in node_json:
            if node.get("id") == node_id:
                return node.get("app_data").get("label").strip()
            else:
                continue

    @staticmethod
    def _parameters_parse(input_parameters: list):
        format_input_parameters = []
        for item in input_parameters:
            input_parameter = {}
            if "name" in item:
                input_parameter["name"] = item.get("name").strip()
            input_parameter["type"] = item.get("type").get("widget")
            if "value" in item.get("type"):
                input_parameter["value"] = item.get("type").get("value")
            if "description" in item:
                input_parameter["description"] = item.get("description")
            format_input_parameters.append(input_parameter)
        return format_input_parameters

    @staticmethod
    def _parse_trigger_parameters(
        node: dict,
        node_type,
        linknodes_name_to_type: dict,
        response,
    ):
        trigger_parameters_field = []
        node_id = node.get("id")
        node_name = node.get("app_data").get("label").strip()
        data = {
            "nodeType": node_type,
            "nodeID": node_id,
            "nodeName": node_name,
            "propertyName": "",
            "index": 0,
        }
        trigger_parameters = node.get("app_data").get("component_parameters").get("trigger_parameters")
        for index, item in enumerate(trigger_parameters):
            data["index"] = index + 1
            temp_item = {}
            key = "value"
            if data.get("nodeType") == "K8s Object Trigger":
                key = "src"
                temp_item["src"] = ""
                temp_item["dest"] = "{{" + str(item.get("dest")) + "}}"
            else:
                temp_item["name"] = item.get("name")
                temp_item["value"] = ""
            value = item.get("from").get("value").strip()
            widget = item.get("from").get("widget")

            if widget == "workflow_enum":
                temp_item[key] = "{{" + value + "}}"
            elif widget == "event_enum":
                name = value.split(":")[0]
                if name in linknodes_name_to_type:
                    temp_item[key] = "{{events." + linknodes_name_to_type[name] + "." + value + "}}"
                    temp_item[key] = temp_item[key].replace(": ", ".")
                else:
                    data["propertyName"] = "From of " + data.get("nodeType") + " Parameters"
                    response.add_message(
                        severity=ValidationSeverity.Error,
                        message_type="invalidNodePropertyValue",
                        message="This node is not connected to the Event named "
                        + name
                        + ". Please check the node connection.",
                        runtime="WORKFLOW",
                        data=data,
                    )
            else:
                temp_item[key] = value
            trigger_parameters_field.append(temp_item)
        return trigger_parameters_field

    @staticmethod
    def _get_k8s_agruments(parameters: list, node_json: dict):
        field_map = {
            "model_event": "model",
            "s3_event": "s3",
            "calendar_event": "calendar",
            "dataset_event": "dataset",
            "model_monitor_event": "modelMonitor",
            "pipeline_trigger": "pipeline",
            "k8s_object_trigger": "k8sobj",
            "http_trigger": "http",
            "init": "init",
            "exit": "exit",
        }
        for node in node_json:
            field_map[node.get("app_data").get("label").strip()] = field_map[node.get("op").split(":")[0]]
        arguments_list = []
        for dict_item in parameters:
            temp_argument = {}
            if dict_item.get("from").get("widget") == "workflow_enum":
                temp_argument["src"] = "{{" + dict_item.get("from").get("value").strip() + "}}"
            elif dict_item.get("from").get("widget") == "event_enum":
                name = dict_item.get("from").get("value").split(":")[0]
                temp_argument["src"] = "{{events." + field_map[name] + "." + dict_item.get("from").get("value") + "}}"
                temp_argument["src"] = temp_argument.get("src").replace(": ", ".")
            else:
                temp_argument["src"] = dict_item.get("from").get("value")
            temp_argument["dest"] = "{{%s}}" % dict_item.get("dest")
            arguments_list.append(temp_argument)
        return arguments_list

    @staticmethod
    def _get_k8s_source(parameters: dict):
        if parameters.get("widget") == "s3":
            return {
                "s3": {
                    "bucket": {"name": parameters.get("bucket_name").strip()},
                    "object": parameters.get("object").strip(),
                }
            }
        elif parameters.get("widget") == "http":
            return {"http": {"url": parameters.get("url").strip()}}
        else:
            return {}

    @staticmethod
    def file2zip(zip_file_name: str, file_names: list):
        with zipfile.ZipFile(zip_file_name, mode="w", compression=zipfile.ZIP_DEFLATED) as z_file:
            for file_name in file_names:
                _, name = os.path.split(file_name)
                z_file.write(file_name, arcname=name)

    async def export_custom(self, root, parent, node_json: dict, export_path: str, overwrite: bool):
        response = ValidationResponse(runtime="WORKFLOW")
        zip_file_name = ""
        nodes = node_json.get("pipelines")[0].get("nodes")
        app_data_properties = node_json.get("pipelines")[0].get("app_data").get("properties")
        name = Path(str(export_path)).stem
        parameters_field = []
        file_list = []
        runtime_config = node_json.get("pipelines")[0].get("app_data").get("runtime_config")
        if app_data_properties.__contains__("input_parameters"):
            parameters_field = self._parameters_parse(app_data_properties.get("input_parameters"))

        response = self._validate(nodes, parameters_field)
        if not response.has_fatal:
            para_field = await self._component_parse(root, parent, nodes, runtime_config, file_list)
            response = para_field[4]
            if not response.has_fatal:
                spec_field = self._sepc_parse(
                    para_field[0],
                    para_field[3],
                    parameters_field,
                    para_field[1],
                    para_field[2],
                )
                workflow_yaml = {
                    "apiVersion": "wfe.hiascend.com/v1",
                    "kind": "Feature",
                    "metadata": {"name": name},
                    "spec": spec_field,
                }
                save_path = export_path.replace(".yaml", "-workflow.yaml")
                file_fd = os.open(save_path, os.O_RDWR | os.O_CREAT | os.O_TRUNC, 0o666)
                with os.fdopen(file_fd, "w") as file:
                    file.write(yaml.dump(workflow_yaml, allow_unicode=True, sort_keys=False))
                file_list.append(save_path)
                zip_file_name = save_path.replace(".yaml", ".zip")
                self.file2zip(zip_file_name, file_list)
        return zip_file_name, response

    async def upload(self, file_path: str, runtime_config: str, name: str, description: str):
        response = ValidationResponse(runtime="WORKFLOW")
        runtime_configuration = self._get_metadata_configuration(
            schemaspace=Runtimes.RUNTIMES_SCHEMASPACE_ID, name=runtime_config
        )
        api_endpoint = runtime_configuration.metadata.get("api_endpoint")
        url = api_endpoint + "/apis/v1beta1/workflows/upload"
        files = {"uploadfile": open(file_path, "rb")}
        values = {"name": name, "description": description}
        result = requests.post(url, files=files, params=values)
        message = ""
        if result.status_code != 200:
            if result.status_code == 502:
                message = "502 Bad Gateway."
            else:
                content = eval(str(result.content, encoding="utf-8"))
                message = content.get("error")
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="uploadFailed",
                message=message,
                runtime="WORKFLOW",
                data={"status_code": result.status_code},
            )
        return response

    def _validate(self, nodes, input_parameters):
        response = ValidationResponse(runtime="WORKFLOW")
        name_dict = {}
        wf_input_paras = []
        validator = {
            "model_event": self._validate_model_event,
            "s3_event": self._validate_s3_event,
            "calendar_event": self._validate_calendar_event,
            "dataset_event": self._validate_dataset_event,
            "model_monitor_event": self._validate_model_monitor_event,
            "pipeline_trigger": self._validate_pipeline_trigger,
            "k8s_object_trigger": self._validate_k8s_object_trigger,
            "http_trigger": self._validate_http_trigger,
            "init": self._validate_init,
            "exit": self._validate_exit,
        }
        for index, parameter in enumerate(input_parameters):
            if ("name" not in parameter) or (parameter.get("name").strip() == ""):
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodeProperty",
                    message=f"The 'Parameter Name' field of {index + 1}-th workflow input parameters cannot be empty.",
                    runtime="WORKFLOW",
                )
            elif not re.match(r"^[a-zA-Z][a-zA-Z0-9_]{0,62}$", parameter.get("name")):
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodeProperty",
                    message=f"The 'Parameter Name' field of {index + 1}-th workflow input parameter "
                    + "does not match the regular expression ^[a-zA-Z][a-zA-Z0-9_{{0,62}}]$.",
                    runtime="WORKFLOW",
                )
            else:
                wf_input_paras.append("workflow.parameters." + parameter.get("name").strip())
        for node in nodes:
            node_type = node.get("op").split(":")[0]
            validate_func = validator[node_type]
            validate_func(node, wf_input_paras, name_dict, response)
        return response

    def _validate_s3_event(self, node, wf_input_paras, name, response):
        node_type = "S3 Event"
        node_id = node.get("id")
        node_name = node.get("app_data").get("label").strip()
        self._validate_node_name(node, name, response)
        bucket_name = node.get("app_data").get("component_parameters").get("bucket_name")
        self._validate_node_property_value(
            bucket_name, node_type, node_id, node_name, "Bucket Name", wf_input_paras, response
        )
        s3_object = {
            "S3 Object Prefix": node.get("app_data").get("component_parameters").get("prefix"),
            "S3 Object Suffix": node.get("app_data").get("component_parameters").get("suffix"),
        }
        for property_name, property_value in s3_object.items():
            if property_value.get("widget") == "enum":
                value = property_value.get("value").strip()
                if (value != "") and (value not in wf_input_paras):
                    response.add_message(
                        severity=ValidationSeverity.Error,
                        message_type="invalidNodePropertyValue",
                        message="Workflow input parameters do not contain '" + value + "'.",
                        runtime="WORKFLOW",
                        data={
                            "nodeType": node_type,
                            "nodeID": node_id,
                            "nodeName": node_name,
                            "propertyName": property_name,
                        },
                    )
        if ("event_filter" not in node.get("app_data").get("component_parameters")) or (
            len(node.get("app_data").get("component_parameters").get("event_filter")) == 0
        ):
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data={
                    "nodeType": node_type,
                    "nodeID": node_id,
                    "nodeName": node_name,
                    "propertyName": "S3 Event Filters",
                },
            )
        else:
            event_filters = node.get("app_data").get("component_parameters").get("event_filter")
            for index, event_filter in enumerate(event_filters):
                self._validate_event_filter(
                    index, event_filter, node_type, node_id, node_name, "S3 Event Filters", wf_input_paras, response
                )

    def _validate_calendar_event(self, node, wf_input_paras, name, response):
        node_type = "Calendar Event"
        node_id = node.get("id")
        node_name = node.get("app_data").get("label").strip()
        self._validate_node_name(node, name, response)
        calendar = node.get("app_data").get("component_parameters").get("calendar")
        self._validate_node_property_value(
            calendar.get("value"),
            node_type,
            node_id,
            node_name,
            calendar.get("name"),
            wf_input_paras,
            response,
        )

    def _validate_model_event(self, node, wf_input_paras, name, response):
        self._validate_dataset_model_event(node, wf_input_paras, name, response, "Model")

    def _validate_dataset_event(self, node, wf_input_paras, name, response):
        self._validate_dataset_model_event(node, wf_input_paras, name, response, "Dataset")

    def _validate_model_monitor_event(self, node, wf_input_paras, name, response):
        node_type = "Model Monitor Event"
        node_id = node.get("id")
        node_name = node.get("app_data").get("label").strip()
        self._validate_node_name(node, name, response)
        if ("event_filter" not in node.get("app_data").get("component_parameters")) or (
            len(node.get("app_data").get("component_parameters").get("event_filter")) == 0
        ):
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data={
                    "nodeType": node_type,
                    "nodeID": node_id,
                    "nodeName": node_name,
                    "propertyName": "Model Configuration",
                },
            )
        else:
            event_filters = node.get("app_data").get("component_parameters").get("event_filter")
            for index, event_filter in enumerate(event_filters):
                self._validate_node_property_value(
                    event_filter.get("app_name"),
                    node_type,
                    node_id,
                    node_name,
                    "App Name of Model Configuration with Id=" + str(event_filter.get("id")),
                    wf_input_paras,
                    response,
                    index,
                )
                self._validate_node_property_value(
                    event_filter.get("model_name"),
                    node_type,
                    node_id,
                    node_name,
                    "Model Name of Model Configuration with Id=" + str(event_filter.get("id")),
                    wf_input_paras,
                    response,
                    index,
                )

    def _validate_pipeline_trigger(self, node, wf_input_paras, name, response):
        node_type = "Pipeline Trigger"
        node_id = node.get("id")
        node_name = node.get("app_data").get("label").strip()
        self._validate_node_name(node, name, response)
        self._validate_node_links(node, node_type, node_id, node_name, response)
        path = node.get("app_data").get("component_parameters").get("template_name")
        self._validate_filepath(node_type, node_id, node_name, "Template Name", path, response)
        if "trigger_parameters" not in node.get("app_data").get("component_parameters"):
            node["app_data"]["component_parameters"]["trigger_parameters"] = []
        else:
            trigger_parameters = node.get("app_data").get("component_parameters").get("trigger_parameters")
            self._validate_trigger_parameters(
                trigger_parameters,
                node_type,
                node_id,
                node_name,
                "Pipeline Trigger Parameters",
                wf_input_paras,
                response,
            )

    def _validate_k8s_object_trigger(self, node, wf_input_paras, name, response):
        node_type = "K8s Object Trigger"
        node_id = node.get("id")
        node_name = node.get("app_data").get("label").strip()
        self._validate_node_name(node, name, response)
        self._validate_node_links(node, node_type, node_id, node_name, response)
        source = node.get("app_data").get("component_parameters").get("source")
        data = {
            "nodeType": node_type,
            "nodeID": node_id,
            "nodeName": node_name,
            "propertyName": "",
        }
        if source.get("widget") == "s3":
            if ("bucket_name" not in source) or (source.get("bucket_name").strip() == ""):
                data["propertyName"] = "Bucket Name of Source s3"
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Node is missing a value for a required property.",
                    runtime="WORKFLOW",
                    data=data,
                )
            if ("object" not in source) or (source.get("object").strip() == ""):
                data["propertyName"] = "Object of Source s3"
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Node is missing a value for a required property.",
                    runtime="WORKFLOW",
                    data=data,
                )
        elif source.get("widget") == "http":
            if ("url" not in source) or (source.get("url").strip() == ""):
                data["propertyName"] = "Url of Source http"
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Node is missing a value for a required property.",
                    runtime="WORKFLOW",
                    data=data,
                )
        if "trigger_parameters" not in node.get("app_data").get("component_parameters"):
            node["app_data"]["component_parameters"]["trigger_parameters"] = []
        else:
            trigger_parameters = node.get("app_data").get("component_parameters").get("trigger_parameters")
            para_name = "K8s Object Trigger Parameters"
            self._validate_trigger_parameters(
                trigger_parameters, node_type, node_id, node_name, para_name, wf_input_paras, response
            )

    def _validate_http_trigger(self, node, wf_input_paras, name, response):
        node_type = "HTTP Trigger"
        node_id = node.get("id")
        node_name = node.get("app_data").get("label").strip()
        self._validate_node_name(node, name, response)
        self._validate_node_links(node, node_type, node_id, node_name, response)

        if ("url" not in node.get("app_data").get("component_parameters")) or (
            node.get("app_data").get("component_parameters").get("url").strip() == ""
        ):
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data={
                    "nodeType": node_type,
                    "nodeID": node_id,
                    "nodeName": node_name,
                    "propertyName": "Url of HTTP Trigger",
                },
            )
        if "trigger_parameters" not in node.get("app_data").get("component_parameters"):
            node["app_data"]["component_parameters"]["trigger_parameters"] = []
        else:
            trigger_parameters = node.get("app_data").get("component_parameters").get("trigger_parameters")
            self._validate_trigger_parameters(
                trigger_parameters,
                node_type,
                node_id,
                node_name,
                "HTTP Trigger Parameters",
                wf_input_paras,
                response,
            )

    def _validate_init(self, node, wf_input_paras, name, response):
        node_type = "Init"
        node_id = node.get("id")
        node_name = node.get("app_data").get("label").strip()
        if "init_parameters" not in node.get("app_data").get("component_parameters"):
            node["app_data"]["component_parameters"]["init_parameters"] = []
        path = node.get("app_data").get("component_parameters").get("init_pipeline")
        self._validate_filepath(node_type, node_id, node_name, "Init Pipeline File", path, response)
        init_parameters = node.get("app_data").get("component_parameters").get("init_parameters")
        self._validate_trigger_parameters(
            init_parameters,
            node_type,
            node_id,
            node_name,
            "Init Parameters",
            wf_input_paras,
            response,
        )

    def _validate_exit(self, node, wf_input_paras, name, response):
        node_type = "Exit"
        node_id = node.get("id")
        node_name = node.get("app_data").get("label").strip()
        if "exit_parameters" not in node.get("app_data").get("component_parameters"):
            node["app_data"]["component_parameters"]["exit_parameters"] = []
        path = node.get("app_data").get("component_parameters").get("exit_pipeline")
        self._validate_filepath(node_type, node_id, node_name, "Exit Pipeline File", path, response)
        exit_parameters = node.get("app_data").get("component_parameters").get("exit_parameters")
        self._validate_trigger_parameters(
            exit_parameters,
            node_type,
            node_id,
            node_name,
            "Exit Parameters",
            wf_input_paras,
            response,
        )

    def _validate_dataset_model_event(self, node, wf_input_paras, name, response, node_type):
        node_id = node.get("id")
        node_name = node.get("app_data").get("label").strip()
        data = {
            "nodeType": "{} Event".format(node_type),
            "nodeID": node_id,
            "nodeName": node_name,
            "propertyName": "",
        }
        self._validate_node_name(node, name, response)
        if ("{}_name".format(node_type.lower()) not in node.get("app_data").get("component_parameters")) or (
            len(node.get("app_data").get("component_parameters").get("{}_name".format(node_type.lower()))) == 0
        ):
            data["propertyName"] = "{} Names".format(node_type)
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        else:
            names = node.get("app_data").get("component_parameters").get("{}_name".format(node_type.lower()))
            for index, name in enumerate(names):
                self._validate_node_property_value(
                    name, node_type, node_id, node_name, "{} Name".format(node_type), wf_input_paras, response, index
                )
        if ("event_filter" not in node.get("app_data").get("component_parameters")) or (
            len(node.get("app_data").get("component_parameters").get("event_filter")) == 0
        ):
            data["propertyName"] = "{} Event Filters".format(node_type)
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        else:
            component_parameters = node.get("app_data").get("component_parameters")
            default_expression = self._get_default_expression(
                component_parameters, node_type, node_id, node_name, wf_input_paras, response
            )
            expression = component_parameters.get("expression")
            if "expression" not in component_parameters or expression == "":
                component_parameters["expression"] = default_expression
            else:
                max_num = len(node.get("app_data").get("component_parameters").get("event_filter"))
                self._validate_expression(node_type, node_id, node_name, max_num, expression, response)

    def _validate_expression(self, node_type, node_id, node_name, max_num, expression, response):
        pattern = r"^([1-9]+|\([1-9]+((&&|\|\|)[1-9]+)+\))((&&|\|\|)([1-9]+|\([1-9]+((&&|\|\|)[1-9]+)+\)))*$"
        match_result = re.match(pattern, expression)
        if not match_result:
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Incorrect expression format.",
                runtime="WORKFLOW",
                data={
                    "nodeType": "{} Event".format(node_type),
                    "nodeID": node_id,
                    "nodeName": node_name,
                    "propertyName": "Expression",
                },
            )
        else:
            pattern = r"\d+"
            match_num = [int(num) for num in re.findall(pattern, expression)]
            if any(num < 1 or num > max_num for num in match_num):
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="The index value appearing in the expression is not within "
                    + "the range of {} Event Filter.".format(node_type),
                    runtime="WORKFLOW",
                    data={
                        "nodeType": "{} Event".format(node_type),
                        "nodeID": node_id,
                        "nodeName": node_name,
                        "propertyName": "Expression",
                    },
                )

    def _get_default_expression(self, component_parameters, node_type, node_id, node_name, wf_input_paras, response):
        event_filters = component_parameters.get("event_filter")
        filter_id = 1
        expression = "1"
        for index, event_filter in enumerate(event_filters):
            self._validate_event_filter(
                index,
                event_filter,
                node_type,
                node_id,
                node_name,
                "{} Event Filters".format(node_type),
                wf_input_paras,
                response,
            )
            if filter_id > 1:
                expression += "&&" + str(filter_id)
            filter_id += 1
        return expression

    def _validate_filepath(
        self,
        node_type,
        node_id,
        node_name,
        property_name,
        path,
        response,
    ):
        if not os.path.exists(path) or not os.path.isfile(path):
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidFilePath",
                message="Property has an invalid path to a file/dir or the file/dir does not exist.",
                data={
                    "nodeType": node_type,
                    "nodeID": node_id,
                    "nodeName": node_name,
                    "propertyName": property_name,
                },
            )

    def _validate_node_name(self, node, name, response):
        label = node.get("app_data").get("label").strip()
        if label in name:
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodeName",
                message="Workflow component name fields cannot be the same",
                runtime="WORKFLOW",
                data={
                    "duplicateName": label,
                    "node1": name[label],
                    "node2": node.get("id"),
                },
            )
        else:
            name[label] = node.get("id")

    def _validate_node_property_value(
        self,
        node_property,
        node_type,
        node_id,
        node_name,
        property_name,
        wf_input_paras,
        response,
        index=-1,
    ):
        data = {
            "nodeType": node_type,
            "nodeID": node_id,
            "nodeName": node_name,
            "propertyName": property_name,
        }
        if index != -1:
            data["index"] = index + 1
        if ("value" not in node_property) or (node_property.get("value").strip() == ""):
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        elif node_property.get("widget") == "enum":
            value = node_property.get("value").strip()
            if value not in wf_input_paras:
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Workflow input parameters do not contain '" + value + "'.",
                    runtime="WORKFLOW",
                    data=data,
                )

    def _validate_event_filter(
        self,
        index,
        node_property,
        node_type,
        node_id,
        node_name,
        property_name,
        wf_input_paras,
        response,
    ):
        data = {
            "nodeType": node_type,
            "nodeID": node_id,
            "nodeName": node_name,
            "propertyName": "",
            "index": index + 1,
        }
        if "name" not in node_property:
            data["propertyName"] = "Property Name of " + property_name
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        if "operate" not in node_property:
            data["propertyName"] = "Operate of " + property_name
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        if str(node_property.get("value").get("value")).strip() == "":
            data["propertyName"] = "Value of " + property_name
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        elif node_property.get("value").get("widget") == "enum":
            data["propertyName"] = "Value of " + property_name
            value = node_property.get("value").get("value").strip()
            if value not in wf_input_paras:
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Workflow input parameters do not contain '" + value + "'.",
                    runtime="WORKFLOW",
                    data=data,
                )

    def _validate_node_links(self, node, node_type, node_id, node_name, response):
        if "links" not in node.get("inputs")[0]:
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNode",
                message="This trigger node has no links.",
                runtime="WORKFLOW",
                data={
                    "nodeType": node_type,
                    "nodeID": node_id,
                    "nodeName": node_name,
                },
            )

    def _validate_trigger_parameters(
        self,
        parameters,
        node_type,
        node_id,
        node_name,
        property_name,
        wf_input_paras,
        response,
    ):
        data = {
            "nodeType": node_type,
            "nodeID": node_id,
            "nodeName": node_name,
            "propertyName": "",
            "index": 0,
        }
        name = "name"
        value = "value"
        if node_type in ["Pipeline Trigger", "HTTP Trigger"]:
            name = "name"
            value = "from"
        elif node_type == "K8s Object Trigger":
            name = "dest"
            value = "from"

        for index, parameter in enumerate(parameters):
            data["index"] = index + 1
            if (name not in parameter) or (parameter[name].strip() == ""):
                data["propertyName"] = name.title() + " of " + property_name
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Node is missing a value for a required property.",
                    runtime="WORKFLOW",
                    data=data,
                )
            if ("value" not in parameter[value]) or (parameter[value].get("value").strip() == ""):
                data["propertyName"] = value.title() + " of " + property_name
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Node is missing a value for a required property.",
                    runtime="WORKFLOW",
                    data=data,
                )
            elif parameter[value].get("widget") in ["workflow_enum", "enum"]:
                if (
                    parameter[value].get("value") not in wf_input_paras
                    and parameter[value].get("value") != "workflow.instance_name"
                ):
                    data["propertyName"] = value.title() + " of " + property_name
                    response.add_message(
                        severity=ValidationSeverity.Error,
                        message_type="invalidNodePropertyValue",
                        message="Workflow input parameters do not contain '" + parameter[value].get("value") + "'.",
                        runtime="WORKFLOW",
                        data=data,
                    )


class WfpPipelineProcessorResponse(RuntimePipelineProcessorResponse):

    _type = RuntimeProcessorType.WORKFLOW_PIPELINES
    _name = "wfp"
