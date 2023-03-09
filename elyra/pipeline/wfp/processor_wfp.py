from elyra.pipeline.processor import RuntimePipelineProcessorResponse
from elyra.pipeline.processor import RuntimePipelineProcessor
from elyra.pipeline.runtime_type import RuntimeProcessorType
from elyra.pipeline.validation import PipelineValidationManager
from elyra.pipeline.validation import ValidationResponse
from elyra.pipeline.validation import ValidationSeverity
from elyra.pipeline.processor import PipelineProcessorManager
from elyra.pipeline.parser import PipelineParser
from elyra.metadata.schemaspaces import Runtimes
from pathlib import Path
from yaml.resolver import BaseResolver
from ruamel.yaml import YAML
from ruamel.yaml.scalarstring import PreservedScalarString as pss

import yaml
import json
import re
import os
import zipfile
import requests
from elyra.util.validation_type import *


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
            "metadata": {
                "name": name,
                "namespace": "kubeflow"
            },
            "spec": {
                "resource": str_yaml
            }
        }
        save_path = path.replace(".yaml", "-pipeline.yaml")
        with open(save_path, "w") as file:
            yaml_loader.dump(pipeline_template, file)
        file_list.append(save_path)
    
    async def export_pipeline(self, path, runtime_config, root_dir, parent, file_list):
        response = ValidationResponse()
        pipeline_input_parameters = []
        resource = ""
        if path.endswith(".pipeline"):
            pipeline_file = open(path, 'r', encoding='UTF-8')
            pipeline_definition = json.load(pipeline_file)
            pipeline_definition["pipelines"][0]["app_data"]["runtime_config"] = runtime_config
            pipeline_definition["pipelines"][0]["app_data"]["runtime"] = "kfp"
            pipeline_definition["pipelines"][0]["app_data"]["name"] = Path(str(path)).stem
            pipeline_definition["pipelines"][0]["app_data"]["source"] = path
            
            response = await PipelineValidationManager.instance().validate(pipeline_definition)
            
            if not response.has_fatal:
                pipeline = PipelineParser(root_dir=root_dir, parent=parent).parse(
                    pipeline_definition
                )
                await PipelineProcessorManager.instance().export(
                    pipeline, "yaml", path.replace(".pipeline", ".yaml"), True
                )
                pipeline_yaml = open(path.replace(".pipeline", ".yaml"), "r", encoding='utf-8')
                resource = pipeline_yaml.read()
                self.create_pipeline_template(path.replace(".pipeline", ".yaml"), resource, file_list)
                # os.remove(path.replace(".pipeline", ".yaml"))
        elif path.endswith(".yaml"):
            pipeline_yaml = open(path, "r", encoding='utf-8')
            resource = pipeline_yaml.read()
            self.create_pipeline_template(path, resource, file_list)
        if not response.has_fatal:
            export_pipeline_yaml = yaml.load(resource, Loader=yaml.FullLoader)
            if "pipelines.kubeflow.org/pipeline_spec" in export_pipeline_yaml["metadata"]["annotations"]:
                pipeline_spec_dict = json.loads(export_pipeline_yaml["metadata"]["annotations"]["pipelines.kubeflow.org/pipeline_spec"])
                if "inputs" in pipeline_spec_dict:
                    for item in pipeline_spec_dict["inputs"]:
                        pipeline_input_parameter = {}
                        pipeline_input_parameter["name"] = item["name"]
                        pipeline_input_parameter["value"] = item["default"]
                        pipeline_input_parameters.append(pipeline_input_parameter)
        
        return response, pipeline_input_parameters

    @staticmethod
    def _sepc_parse(init: dict, exit_parameters: dict, parameters: list, events: dict, triggers: dict):
        spec_field = {}
        if init and init["pipeline"]:
            spec_field["init"] = init
        if exit_parameters and exit_parameters["pipeline"]:
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
        eventFilter = []
        for item in event_filter:
            temp_filter = {}
            temp_filter["AppName"] = self._widget_value_str(item["app_name"])
            temp_filter["ModelName"] = self._widget_value_str(item["model_name"])
            eventFilter.append(temp_filter)
        return eventFilter

    async def _component_parse(self, root_dir, parent, node_json: dict, runtime_config, export_path, file_list):
        # key: catalog type
        # value: workflow.yaml field
        response = ValidationResponse(runtime="WORKFLOW")
        init_field = {}
        exit_field = {}
        event_field = {}
        trigger_field = {}
        for node in node_json:
            node_type = node["op"].split(":")[0]
            if node_type == "model_event":
                if "model" not in event_field:
                    event_field["model"] = {}

                event_field["model"][node["app_data"]["label"].strip()] = {
                    "models": self._get_dataset_or_model_names(node["app_data"]["component_parameters"]["model_name"]),
                    "eventFilter": {
                        "expression": self._get_event_filter(node["app_data"]["component_parameters"]["event_filter"],
                                                             node["app_data"]["component_parameters"]["expression"])
                    }
                }
            elif node_type == "s3_event":
                if "s3" not in event_field:
                    event_field["s3"] = {}

                event_field["s3"][node["app_data"]["label"].strip()] = {
                    "bucket": {
                        "name": self._widget_value_str(node["app_data"]["component_parameters"]["bucket_name"])
                    },
                    "eventFilter": {
                        "expression": self._get_s3_event_filter(node["app_data"]["component_parameters"])
                    }
                }
            elif node_type == "calendar_event":
                if "calendar" not in event_field:
                    event_field["calendar"] = {}

                calendar = node["app_data"]["component_parameters"]["calendar"]
                event_field["calendar"][node["app_data"]["label"].strip()] = {
                    calendar["name"]: self._widget_value_str(calendar["value"])
                }
            elif node_type == "dataset_event":
                if "dataset" not in event_field:
                    event_field["dataset"] = {}

                event_field["dataset"][node["app_data"]["label"].strip()] = {
                    "datasets": self._get_dataset_or_model_names(node["app_data"]["component_parameters"]["dataset_name"]),
                    "eventFilter": {
                        "expression": self._get_event_filter(
                            node["app_data"]["component_parameters"]["event_filter"],
                            node["app_data"]["component_parameters"]["expression"]
                            )
                    }
                }
            elif node_type == "model_monitor_event":
                if "modelMonitor" not in event_field:
                    event_field["modelMonitor"] = {}

                event_field["modelMonitor"][node["app_data"]["label"].strip()] = {
                    "alertName": self._widget_value_str(node["app_data"]["component_parameters"]["alert_name"]),
                    "eventFilter": self._get_model_monitor_event_filter(node["app_data"]["component_parameters"]["event_filter"])
                }  
            elif node_type == "pipeline_trigger":
                if "pipeline" not in trigger_field:
                    trigger_field["pipeline"] = {}

                node_type = "Pipeline Trigger"
                node_id = node["id"]
                node_name = node["app_data"]["label"].strip()
                data = {
                    "nodeType": node_type,
                    "nodeID": node_id,
                    "nodeName": node_name,
                    "propertyName": "",
                    "index": 0
                }
                condition, linknodes_name_to_type = self._get_condition(node_json, node)
                parameters = self._parse_trigger_parameters(
                    node["app_data"]["component_parameters"]["trigger_parameters"],
                    node_json,
                    data,
                    linknodes_name_to_type,
                    response
                    )
                path = node["app_data"]["component_parameters"]["template_name"]
                pipeline_response, pipeline_input_parameters = await self.export_pipeline(path, runtime_config, root_dir, parent, file_list)
                if pipeline_response.has_fatal:
                    issues = (pipeline_response.response)["issues"]
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
                                "issue": issue
                            },
                        )
                    return [init_field, event_field, trigger_field, exit_field, response]
                for parameter in parameters:
                    for pipeline_input_parameter in pipeline_input_parameters:
                        if pipeline_input_parameter["name"] == parameter["name"]:
                            pipeline_input_parameters.remove(pipeline_input_parameter)
                            break
                parameters += pipeline_input_parameters
                condition, _ = self._get_condition(node_json, node)
                trigger_field["pipeline"][node["app_data"]["label"].strip()] = {
                    "condition": condition,
                    "operation": "create",
                    "pipeline": {
                        "pipelineTemplate": Path(str(node["app_data"]["component_parameters"]["template_name"])).stem,
                        "parameters": parameters
                    }
                }
                data = {
                    "workflowSource": "Pipeline Trigger",
                    "pipelineTriggerName": node["app_data"]["label"].strip()
                }
            elif node_type == "k8s_object_trigger":
                if "k8sobj" not in trigger_field:
                    trigger_field["k8sobj"] = {}

                node_type = "K8s Object Trigger"
                node_id = node["id"]
                node_name = node["app_data"]["label"].strip()
                data = {
                    "nodeType": node_type,
                    "nodeID": node_id,
                    "nodeName": node_name,
                    "propertyName": "",
                    "index": 0
                }

                condition, linknodes_name_to_type = self._get_condition(node_json, node)
                parameters = self._parse_trigger_parameters(
                    node["app_data"]["component_parameters"]["trigger_parameters"],
                    node_json,
                    data,
                    linknodes_name_to_type,
                    response
                    )
                trigger_field["k8sobj"][node["app_data"]["label"].strip()] = {
                    "condition": condition,
                    "operation": node["app_data"]["component_parameters"]["operation"],
                    "source": self._get_k8s_source(node["app_data"]["component_parameters"]["source"]),
                    "arguments": parameters
                }
            elif node_type == "http_trigger":
                if "http" not in trigger_field:
                    trigger_field["http"] = {}
                
                node_type = "HTTP Trigger"
                node_id = node["id"]
                node_name = node["app_data"]["label"].strip()
                data = {
                    "nodeType": node_type,
                    "nodeID": node_id,
                    "nodeName": node_name,
                    "propertyName": "",
                    "index": 0
                }
                condition, linknodes_name_to_type = self._get_condition(node_json, node)
                parameters = self._parse_trigger_parameters(
                    node["app_data"]["component_parameters"]["trigger_parameters"],
                    node_json,
                    data,
                    linknodes_name_to_type,
                    response
                    )

                trigger_field["http"][node["app_data"]["label"].strip()] = {
                    "condition": condition,
                    "url": node["app_data"]["component_parameters"]["url"],
                    "method": node["app_data"]["component_parameters"]["method"],
                    "timeout": str(node["app_data"]["component_parameters"]["timeout"]) + 's',
                    "payload": parameters,
                }
            elif node_type == "init":
                init_pipeline_field = {}
                pipeline_input_parameters = {}
                data = {
                    "workflowSource": "Init"
                }

                pipeline_path = node["app_data"]["component_parameters"]["init_pipeline"]
                path_name = Path(str(pipeline_path)).stem
                init_pipeline_field["pipelineTemplate"] = path_name
                pipeline_response, pipeline_input_parameters = await self.export_pipeline(pipeline_path, runtime_config, root_dir, parent, file_list)
                if pipeline_response.has_fatal:
                    issues = (pipeline_response.response)["issues"]
                    for issue in issues:
                        response.add_message(
                            severity=ValidationSeverity.Error,
                            message_type="pipelineExportError",
                            message="Pipeline Export Error",
                            runtime="WORKFLOW",
                            data={
                                "nodeType": "Init",
                                "nodeID": node["id"],
                                "issue": issue
                            },
                        )
                    return [init_field, event_field, trigger_field, exit_field, response]

                init_parameters = []
                for init_item in node["app_data"]["component_parameters"]["init_parameters"]:
                    temp_value = self._widget_value_str(init_item["value"])
                    temp_init = {
                        "name": init_item["name"], 
                        "value": temp_value
                    }
                    if "description" in init_item:
                        temp_init["description"] = init_item["description"]
                    init_parameters.append(temp_init)
                for init_parameter in init_parameters:
                    for pipeline_input_parameter in pipeline_input_parameters:
                        if pipeline_input_parameter["name"] == init_parameter["name"]:
                            pipeline_input_parameters.remove(pipeline_input_parameter)
                            break
                init_parameters += pipeline_input_parameters
                init_pipeline_field["parameters"] = init_parameters

                init_pipeline_field = dict(sorted(init_pipeline_field.items(), key=lambda x: x[0], reverse=True))
                init_field = {
                    "pipeline": init_pipeline_field
                }
            elif node_type == "exit":
                exit_pipeline_field = {}
                pipeline_input_parameters = {}
                data = {
                    "workflowSource": "Exit"
                }

                pipeline_path = node["app_data"]["component_parameters"]["exit_pipeline"]
                path_name = Path(str(pipeline_path)).stem
                exit_pipeline_field["pipelineTemplate"] = path_name
                pipeline_response, pipeline_input_parameters = await self.export_pipeline(pipeline_path, runtime_config, root_dir, parent, file_list)
                if pipeline_response.has_fatal:
                    issues = (pipeline_response.response)["issues"]
                    for issue in issues:
                        response.add_message(
                            severity=ValidationSeverity.Error,
                            message_type="pipelineExportError",
                            message="Pipeline Export Error",
                            runtime="WORKFLOW",
                            data={
                                "nodeType": "Exit",
                                "nodeID": node["id"],
                                "issue": issue
                            },
                        )
                    return [init_field, event_field, trigger_field, exit_field, response]

                exit_parameters = []
                for exit_item in node["app_data"]["component_parameters"]["exit_parameters"]:
                    temp_value = self._widget_value_str(exit_item["value"])

                    temp_exit = {
                        "name": exit_item["name"], 
                        "value": temp_value
                    }
                    if "description" in exit_item:
                        temp_exit["description"] = exit_item["description"]
                    exit_parameters.append(temp_exit)
                for exit_parameter in exit_parameters:
                    for pipeline_input_parameter in pipeline_input_parameters:
                        if pipeline_input_parameter["name"] == exit_parameter["name"]:
                            pipeline_input_parameters.remove(pipeline_input_parameter)
                            break
                exit_parameters += pipeline_input_parameters
                exit_pipeline_field["parameters"] = exit_parameters

                exit_pipeline_field = dict(sorted(exit_pipeline_field.items(), key=lambda x: x[0], reverse=True))
                exit_field = {
                    "pipeline": exit_pipeline_field
                }
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
            "http_trigger": "http"
        }
        condition = ""
        condition_num = 0
        linknodes_name_to_type = {}
        for link in node["inputs"][0]["links"]:
            node_type = self._get_type(node_json, link['node_id_ref'])
            node_name = self._get_name(node_json, link['node_id_ref'])
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
        expression = expression.replace(' ', '')
        expression_num = len(re.findall("\d+\.?\d*", expression))
        count = 0
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
                    if count == 0 or expression_num == 1:
                        stack_expression += "(" + expression_item + ") " + char
                        count += 1
                    elif count == expression_item:
                        stack_expression += " (" + expression_item + ")" + char
                        count += 1
                    else:
                        stack_expression += " (" + expression_item + ") " + char
                expression_id = 0
        if expression_id != 0:
            expression_item = self._get_expression_item(expression_id, event_filter)
            if stack_expression == "":
                stack_expression += expression_item
            else:
                stack_expression += " (" + expression_item + ")"
        return stack_expression
    
    def _widget_value_str(self, dict: dict):
        value = dict["value"].strip()
        if value == "":
            return ""
        elif dict["widget"] == "string":
            return value
        elif dict["widget"] == "enum":
            return "{{" + value + "}}"
        else:
            return ""

    def _get_s3_event_filter(self, component_parameters: dict):
        expression = ""
        prefix = self._widget_value_str(component_parameters["prefix"])
        suffix = self._widget_value_str(component_parameters["suffix"])
        event_filter = component_parameters["event_filter"]
        if prefix:
            expression += "object prefix '" + prefix + "'"
        if suffix:
            if expression == "":
                expression += "object suffix '" + suffix + "'"
            else:
                expression = "(" + expression + ") && " + \
                                "(object suffix '" + suffix + "')"
        filter_expression = ""
        for item in event_filter:
            value = self._widget_value_str(item["value"])
            if filter_expression == "":
                filter_expression = item["name"] + " " + item["operate"] + " '" + value + "'"
            else:
                filter_expression = "(" + filter_expression + ") || (" + \
                                        item["name"] + " " + item["operate"] + " '" + value + "')"
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
            expression_item = event_filter[expression_id - 1]["name"] + " "
            if (type(event_filter[expression_id - 1]["value"]) == str):
                expression_item += event_filter[expression_id - 1]["operate"] + " '" + \
                                   str(event_filter[expression_id - 1]["value"]) + "'"
            else:
                if (type(event_filter[expression_id - 1]["value"]["value"]) == int):
                    expression_item += event_filter[expression_id - 1]["operate"] + " " + \
                                       str(event_filter[expression_id - 1]["value"]["value"])
                elif (type(event_filter[expression_id - 1]["value"]["value"]) == str):
                    if (event_filter[expression_id - 1]["value"]["widget"] == "string"):
                        expression_item += event_filter[expression_id - 1]["operate"] + " '" + \
                                           event_filter[expression_id - 1]["value"]["value"] + "'"
                    elif (event_filter[expression_id - 1]["value"]["widget"] == "enum"):
                        expression_item += event_filter[expression_id - 1]["operate"] + " '{{" + \
                                           event_filter[expression_id - 1]["value"]["value"] + "}}'"
        return expression_item

    @staticmethod
    def _get_type(node_json: dict, node_id: str):
        for node in node_json:
            if node['id'] == node_id:
                return node["op"].split(":")[0]
            else:
                continue

    @staticmethod
    def _get_name(node_json: dict, node_id: str):
        for node in node_json:
            if node['id'] == node_id:
                return node["app_data"]["label"].strip()
            else:
                continue

    @staticmethod
    def _parameters_parse(input_parameters: list):
        format_input_parameters = []
        for item in input_parameters:
            input_parameter = {}
            if "name" in item:
                input_parameter["name"] = item["name"].strip()
            input_parameter["type"] = item["type"]["widget"]
            if "value" in item["type"]:
                input_parameter["value"] = item["type"]["value"]
            if "description" in item:
                input_parameter["description"] = item["description"]
            format_input_parameters.append(input_parameter)
        return format_input_parameters

    @staticmethod
    def _parse_trigger_parameters(trigger_parameters: dict, node_json: dict, data: dict, linknodes_name_to_type: dict, response):
        trigger_parameters_field = []

        for index, item in enumerate(trigger_parameters):
            data["index"] = index + 1 
            temp_item = {}
            key = "value"
            if data["nodeType"] == "K8s Object Trigger":
                key = "src"
                temp_item["src"] = ""
                temp_item["dest"] = "{{" + str(item["dest"]) + "}}"
            else:
                temp_item["name"] = item["name"]
                temp_item["value"] = ""
            value = item["from"]["value"].strip()
            widget = item["from"]["widget"]

            if widget == "workflow_enum":
                temp_item[key] = "{{" + value + "}}"
            elif widget == "event_enum":
                name = value.split(":")[0]
                if name in linknodes_name_to_type:
                    temp_item[key] = "{{events." + linknodes_name_to_type[name] + "." + value + "}}"
                    temp_item[key] = temp_item[key].replace(": ", ".")
                else:
                    data["propertyName"] = "From of " + data["nodeType"] + " Parameters"
                    response.add_message(
                        severity=ValidationSeverity.Error,
                        message_type="invalidNodePropertyValue",
                        message="This node is not connected to the Event named " + name + ". Please check the node connection.",
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
            "exit": "exit"
        }
        for node in node_json:
            field_map[node["app_data"]["label"].strip()] = field_map[node["op"].split(":")[0]]
        arguments_list = []
        for dict_item in parameters:
            temp_argument = {}
            if dict_item["from"]["widget"] == "workflow_enum":
                temp_argument["src"] = "{{" + dict_item["from"]["value"].strip() + "}}"
            elif dict_item["from"]["widget"] == "event_enum":
                name = dict_item["from"]["value"].split(":")[0]
                temp_argument["src"] = "{{events." + field_map[name] + "." + dict_item["from"]["value"] + "}}"
                temp_argument["src"] = temp_argument["src"].replace(": ", ".")
            else:
                temp_argument["src"] = dict_item["from"]["value"]
            temp_argument["dest"] = "{{" + str(dict_item["dest"]) + "}}"
            arguments_list.append(temp_argument)
        return arguments_list

    @staticmethod
    def _get_k8s_source(parameters: dict):
        if parameters["widget"] == "s3":
            return {
                "s3": {
                    "bucket": {
                        "name": parameters["bucket_name"].strip()
                    },
                    "object": parameters["object"].strip()
                }
            }
        elif parameters["widget"] == "http":
            return {
                "http": {
                    "url": parameters["url"].strip()
                }
            }
        else:
            return {}

    @staticmethod
    def file2zip(zip_file_name: str, file_names: list):
        with zipfile.ZipFile(zip_file_name, mode='w', compression=zipfile.ZIP_DEFLATED) as zf:
            for fn in file_names:
                parent_path, name = os.path.split(fn)
                zf.write(fn, arcname=name)
    
    async def export_custom(self, root, parent, node_json: dict, export_path: str, overwrite: bool):
        response = ValidationResponse(runtime="WORKFLOW")
        zip_file_name = ""
        nodes = node_json["pipelines"][0]["nodes"]
        app_data_properties = node_json["pipelines"][0]["app_data"]["properties"]
        name = Path(str(export_path)).stem
        parameters_field = []
        file_list = []
        runtime_config = node_json["pipelines"][0]["app_data"]["runtime_config"]
        if app_data_properties.__contains__("input_parameters"):
            parameters_field = self._parameters_parse(app_data_properties["input_parameters"])

        response = self._validate(nodes, parameters_field)
        if not response.has_fatal:
            para_field = await self._component_parse(root, parent, nodes, runtime_config, export_path, file_list)
            response = para_field[4]
            if not response.has_fatal:
                spec_field = self._sepc_parse(para_field[0], para_field[3], parameters_field, para_field[1],
                                            para_field[2])
                workflow_yaml = {
                    "apiVersion": "wfe.hiascend.com/v1",
                    "kind": "Feature",
                    "metadata": {
                        "name": name
                    },
                    "spec": spec_field
                }
                save_path = export_path.replace(".yaml", "-workflow.yaml")
                with open(save_path, "w") as file:
                    file.write(yaml.dump(workflow_yaml, allow_unicode=True, sort_keys=False))
                file_list.append(save_path)
                zip_file_name = save_path.replace(".yaml", ".zip")
                self.file2zip(zip_file_name, file_list)
                return zip_file_name, response

        return zip_file_name, response
    
    async def upload(self, filePath: str, runtime_config: str, name: str, description: str):
        response = ValidationResponse(runtime="WORKFLOW")
        runtime_configuration = self._get_metadata_configuration(
            schemaspace=Runtimes.RUNTIMES_SCHEMASPACE_ID, name=runtime_config
        )
        api_endpoint = runtime_configuration.metadata.get("api_endpoint")
        url = api_endpoint + "/apis/v1beta1/workflows/upload"
        files = {'uploadfile': open(filePath, 'rb')}
        values = {'name': name, 'description': description}
        result = requests.post(url, files=files, params=values)
        message = ""
        if result.status_code != 200:
            if result.status_code == 502:
                message = "502 Bad Gateway."
            else:
                content = eval(str(result.content, encoding = "utf-8"))
                message = content["error"]
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="uploadFailed",
                message=message,
                runtime="WORKFLOW",
                data={"status_code": result.status_code}
            )
        return response

    def _validate(self, nodes, input_parameters):
        response = ValidationResponse(runtime="WORKFLOW")
        name = {}
        workflow_input_parameters = []
        for index, parameter in enumerate(input_parameters):
            if ("name" not in parameter) or (parameter["name"].strip() == "") :
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodeProperty",
                    message="The 'Parameter Name' field of workflow input parameters cannot be empty.",
                    runtime="WORKFLOW",
                    data={"index": index + 1}
                )
            else:
                workflow_input_parameters.append("workflow.parameters." + parameter["name"].strip())
        for node in nodes:
            node_type = node["op"].split(":")[0]
            if node_type == "model_event":
                self._validate_model_event(node, workflow_input_parameters, name, response)
            elif node_type == "s3_event":
                self._validate_s3_event(node, workflow_input_parameters, name, response)
            elif node_type == "calendar_event":
                self._validate_calendar_event(node, workflow_input_parameters, name, response)
            elif node_type == "dataset_event":
                self._validate_dataset_event(node, workflow_input_parameters, name, response)
            elif node_type == "model_monitor_event":
                self._validate_model_monitor_event(node, workflow_input_parameters, name, response)
            elif node_type == "pipeline_trigger":
                self._validate_pipeline_trigger(node, workflow_input_parameters, name, response)
            elif node_type == "k8s_object_trigger":
                self._validate_k8s_object_trigger(node, workflow_input_parameters, name, response)
            elif node_type == "http_trigger":
                self._validate_http_trigger(node, workflow_input_parameters, name, response)
            elif node_type == "init":
                self._validate_init(node, workflow_input_parameters, name, response)
            elif node_type == "exit":
                self._validate_exit(node, workflow_input_parameters, name, response)
        return response
    
    def _validate_s3_event(self, node, workflow_input_parameters, name, response):
        node_type = "S3 Event"
        node_id = node["id"]
        node_name = node["app_data"]["label"].strip()
        self._validate_node_name(node, name, response)
        bucket_name = node["app_data"]["component_parameters"]["bucket_name"]
        s3_object_prefix = node["app_data"]["component_parameters"]["prefix"]
        s3_object_suffix = node["app_data"]["component_parameters"]["suffix"]
        self._validate_node_property_value(bucket_name, node_type, node_id, node_name, "Bucket Name", workflow_input_parameters, response)
        if s3_object_prefix["widget"] == "enum":
            value = s3_object_prefix["value"].strip()
            if (value != "") and (value not in workflow_input_parameters):
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Workflow input parameters do not contain '" + value + "'.",
                    runtime="WORKFLOW",
                    data={
                        "nodeType": node_type,
                        "nodeID": node_id,
                        "nodeName": node_name, 
                        "propertyName": "S3 Object Prefix"
                    },
                )
        if s3_object_suffix["widget"] == "enum":
            value = s3_object_suffix["value"].strip()
            if (value != "") and (value not in workflow_input_parameters):
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Workflow input parameters do not contain '" + value + "'.",
                    runtime="WORKFLOW",
                    data={
                        "nodeType": node_type,
                        "nodeID": node_id,
                        "nodeName": node_name, 
                        "propertyName": "S3 Object Suffix"
                    },
                )

        if ("event_filter" not in node["app_data"]["component_parameters"]) or (len(node["app_data"]["component_parameters"]["event_filter"]) == 0):
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data={
                    "nodeType": node_type,
                    "nodeID": node_id,
                    "nodeName": node_name, 
                    "propertyName": "Dataset Event Filter"
                },
            )
        else:
            event_filters = node["app_data"]["component_parameters"]["event_filter"]
            for index, event_filter in enumerate(event_filters):
                self._validate_event_filter(index, event_filter, node_type, node_id, node_name, "S3 Event Filter", workflow_input_parameters, response)
    
    def _validate_calendar_event(self, node, workflow_input_parameters, name, response):
        node_type = "Calendar Event"
        node_id = node["id"]
        node_name = node["app_data"]["label"].strip()
        self._validate_node_name(node, name, response)
        calendar = node["app_data"]["component_parameters"]["calendar"]
        self._validate_node_property_value(calendar["value"], node_type, node_id, node_name, calendar["name"], workflow_input_parameters, response)
    
    def _validate_model_event(self, node, workflow_input_parameters, name, response):
        node_type = "Model Event"
        node_id = node["id"]
        node_name = node["app_data"]["label"].strip()
        self._validate_node_name(node, name, response)

        data={
            "nodeType": node_type,
            "nodeID": node_id,
            "nodeName": node_name, 
            "propertyName": ""
        }
        
        if ("model_name" not in node["app_data"]["component_parameters"]) or (len(node["app_data"]["component_parameters"]["model_name"]) == 0):
            data["propertyName"] = "Model Name"
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        else:
            model_names = node["app_data"]["component_parameters"]["model_name"]
            for index, model_name in enumerate(model_names):
                self._validate_node_property_value(model_name, node_type, node_id, node_name, "Model Name", workflow_input_parameters, response, index)

        if ("event_filter" not in node["app_data"]["component_parameters"]) or (len(node["app_data"]["component_parameters"]["event_filter"]) == 0):
            data["propertyName"] = "Model Event Filter"
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        else:
            component_parameters = node["app_data"]["component_parameters"]
            event_filters = component_parameters["event_filter"]
            filter_id = 1
            expression = "1"
            for index, event_filter in enumerate(event_filters):
                self._validate_event_filter(index, event_filter, node_type, node_id, node_name, "Model Event Filter", workflow_input_parameters, response)
                if filter_id > 1:
                    expression += "&&" + str(filter_id)
                filter_id += 1
            if "expression" not in component_parameters or component_parameters["expression"].replace(" ", "") == "":
                component_parameters["expression"] = expression
    
    def _validate_dataset_event(self, node, workflow_input_parameters, name, response):
        node_type = "Dataset Event"
        node_id = node["id"]
        node_name = node["app_data"]["label"].strip()
        data = {
            "nodeType": node_type,
            "nodeID": node_id,
            "nodeName": node_name, 
            "propertyName": ""
        }
        self._validate_node_name(node, name, response)
        if ("dataset_name" not in node["app_data"]["component_parameters"]) or (len(node["app_data"]["component_parameters"]["dataset_name"]) == 0):
            data["propertyName"] = "Dataset Name"
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        else:
            dataset_names = node["app_data"]["component_parameters"]["dataset_name"]
            for index, dataset_name in enumerate(dataset_names):
                self._validate_node_property_value(dataset_name, node_type, node_id, node_name, "Dataset Name", workflow_input_parameters, response, index)

        if ("event_filter" not in node["app_data"]["component_parameters"]) or (len(node["app_data"]["component_parameters"]["event_filter"]) == 0):
            data["propertyName"] = "Dataset Event Filter"
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        else:
            component_parameters = node["app_data"]["component_parameters"]
            event_filters = component_parameters["event_filter"]
            filter_id = 1
            expression = "1"
            for index, event_filter in enumerate(event_filters):
                self._validate_event_filter(index, event_filter, node_type, node_id, node_name, "Dataset Event Filter", workflow_input_parameters, response)
                if filter_id > 1:
                    expression += "&&" + str(filter_id)
                filter_id += 1
            if "expression" not in component_parameters or component_parameters["expression"].replace(" ", "") == "":
                component_parameters["expression"] = expression
    
    def _validate_model_monitor_event(self, node, workflow_input_parameters, name, response):
        node_type = "Model Monitor Event"
        node_id = node["id"]
        node_name = node["app_data"]["label"].strip()
        if ("event_filter" not in node["app_data"]["component_parameters"]) or (len(node["app_data"]["component_parameters"]["event_filter"]) == 0):
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data={
                    "nodeType": node_type,
                    "nodeID": node_id,
                    "nodeName": node_name, 
                    "propertyName": "Model Configuration"
                },
            )
        else:
            event_filters = node["app_data"]["component_parameters"]["event_filter"]
            for index, event_filter in enumerate(event_filters):
                self._validate_node_property_value(event_filter["app_name"], node_type, node_id, node_name, "App Name of Model Configuration with Id=" + str(event_filter["id"]), workflow_input_parameters, response, index)
                self._validate_node_property_value(event_filter["model_name"], node_type, node_id, node_name, "Model Name of Model Configuration with Id=" + str(event_filter["id"]), workflow_input_parameters, response, index)

    def _validate_pipeline_trigger(self, node, workflow_input_parameters, name, response):
        node_type = "Pipeline Trigger"
        node_id = node["id"]
        node_name = node["app_data"]["label"].strip()
        self._validate_node_name(node, name, response)
        self._validate_node_links(node, node_type, node_id, node_name, response)
        if "trigger_parameters" not in node["app_data"]["component_parameters"]:
            node["app_data"]["component_parameters"]["trigger_parameters"] = []
        else:
            trigger_parameters = node["app_data"]["component_parameters"]["trigger_parameters"]
            self._validate_trigger_parametes(trigger_parameters, node_type, node_id, node_name, "Pipeline Trigger Parameters", workflow_input_parameters, response)
    
    def _validate_k8s_object_trigger(self, node, workflow_input_parameters, name, response):
        node_type = "K8s Object Trigger"
        node_id = node["id"]
        node_name = node["app_data"]["label"].strip()
        self._validate_node_name(node, name, response)
        self._validate_node_links(node, node_type, node_id, node_name, response)
        source = node["app_data"]["component_parameters"]["source"]
        data={
            "nodeType": node_type,
            "nodeID": node_id,
            "nodeName": node_name, 
            "propertyName": ""
        }
        if source["widget"] == "s3":
            if ("bucket_name" not in source) or (source["bucket_name"].strip() == ""):
                data["propertyName"] = "Bucket Name of Source s3"
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Node is missing a value for a required property.",
                    runtime="WORKFLOW",
                    data=data,
                )
            if ("object" not in source) or (source["object"].strip() == ""):
                data["propertyName"] = "Object of Source s3"
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Node is missing a value for a required property.",
                    runtime="WORKFLOW",
                    data=data,
                )
        elif source["widget"] == "http":
            if ("url" not in source) or (source["url"].strip() == ""):
                data["propertyName"] = "Url of Source http"
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Node is missing a value for a required property.",
                    runtime="WORKFLOW",
                    data=data,
                )
        
        if "trigger_parameters" not in node["app_data"]["component_parameters"]:
            node["app_data"]["component_parameters"]["trigger_parameters"] = []
        else:
            trigger_parameters = node["app_data"]["component_parameters"]["trigger_parameters"]
            self._validate_trigger_parametes(trigger_parameters, node_type, node_id, node_name, "K8s Object Trigger Parameters", workflow_input_parameters, response)
    
    def _validate_http_trigger(self, node, workflow_input_parameters, name, response):
        node_type = "HTTP Trigger"
        node_id = node["id"]
        node_name = node["app_data"]["label"].strip()
        self._validate_node_name(node, name, response)
        self._validate_node_links(node, node_type, node_id, node_name, response)

        if ("url" not in node["app_data"]["component_parameters"]) or (node["app_data"]["component_parameters"]["url"].strip() == ""):
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data={
                    "nodeType": node_type,
                    "nodeID": node_id,
                    "nodeName": node_name, 
                    "propertyName": "Url of HTTP Trigger"
                },
            )
        if "trigger_parameters" not in node["app_data"]["component_parameters"]:
            node["app_data"]["component_parameters"]["trigger_parameters"] = []
        else:
            trigger_parameters = node["app_data"]["component_parameters"]["trigger_parameters"]
            self._validate_trigger_parametes(trigger_parameters, node_type, node_id, node_name, "HTTP Trigger Parameters", workflow_input_parameters, response)
    
    def _validate_init(self, node, workflow_input_parameters, name, response):
        node_type = "Init"
        node_id = node["id"]
        node_name = node["app_data"]["label"].strip()
        if "init_parameters" not in node["app_data"]["component_parameters"]:
            node["app_data"]["component_parameters"]["init_parameters"] = []
        init_parameters = node["app_data"]["component_parameters"]["init_parameters"]
        self._validate_trigger_parametes(init_parameters, node_type, node_id, node_name, "Init Parameters", workflow_input_parameters, response)
    
    def _validate_exit(self, node, workflow_input_parameters, name, response):
        node_type = "Exit"
        node_id = node["id"]
        node_name = node["app_data"]["label"].strip()
        if "exit_parameters" not in node["app_data"]["component_parameters"]:
            node["app_data"]["component_parameters"]["exit_parameters"] = []
        exit_parameters = node["app_data"]["component_parameters"]["exit_parameters"]
        self._validate_trigger_parametes(exit_parameters, node_type, node_id, node_name, "Exit Parameters", workflow_input_parameters, response)
    
    def _validate_node_name(self, node, name, response):
        label = node["app_data"]["label"].strip()
        if label in name:
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodeName",
                message="Workflow component name fields cannot be the same",
                runtime="WORKFLOW",
                data={
                    "duplicateName": label,
                    "node1": name[label],
                    "node2": node["id"]
                }
            )
        else:
            name[label] = node["id"]
    
    def _validate_node_property_value(self, property, nodeType, nodeID, nodeName, propertyName, workflow_input_parameters, response, index = -1):        
        data = {
            "nodeType": nodeType,
            "nodeID": nodeID,
            "nodeName": nodeName, 
            "propertyName": propertyName
        }
        if index != -1:
            data["index"] = index + 1
        if ("value" not in property) or (property["value"].strip() == ""):
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        elif property["widget"] == "enum":
            value = property["value"].strip()
            if value not in workflow_input_parameters:
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Workflow input parameters do not contain '" + value + "'.",
                    runtime="WORKFLOW",
                    data=data,
                )
    
    def _validate_event_filter(self, index, property, nodeType, nodeID, nodeName, propertyName, workflow_input_parameters, response):
        data = {
            "nodeType": nodeType,
            "nodeID": nodeID,
            "nodeName": nodeName, 
            "propertyName": "",
            "index": index + 1
        }
        if "name" not in property:
            data["propertyName"] = "Property Name of " + propertyName
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        if "operate" not in property:
            data["propertyName"] = "Operate of " + propertyName
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        if property["value"]["value"].strip() == "":
            data["propertyName"] = "Value of " + propertyName
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data=data,
            )
        elif property["value"]["widget"] == "enum":
            data["propertyName"] = "Value of " + propertyName
            value = property["value"]["value"].strip()
            if value not in workflow_input_parameters:
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Workflow input parameters do not contain '" + value + "'.",
                    runtime="WORKFLOW",
                    data=data,
                )

    def _validate_node_links(self, node, nodeType, nodeID, nodeName, response):
        if "links" not in node["inputs"][0]:
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNode",
                message="This trigger node has no links.",
                runtime="WORKFLOW",
                data={
                    "nodeType": nodeType,
                    "nodeID": nodeID,
                    "nodeName": nodeName
                },
            )

    def _validate_trigger_parametes(self, parameters, nodeType, nodeID, nodeName, propertyName, workflow_input_parameters, response):
        data = {
            "nodeType": nodeType,
            "nodeID": nodeID,
            "nodeName": nodeName,
            "propertyName": "",
            "index": 0
        }
        name = "name"
        value = "value"
        if nodeType in ["Pipeline Trigger", "HTTP Trigger"]:
            name = "name"
            value = "from"
        elif nodeType == "K8s Object Trigger":
            name = "dest"
            value = "from"

        for index, parameter in enumerate(parameters):
            data["index"] = index + 1
            if (name not in parameter) or (parameter[name].strip() == ""):
                data["propertyName"] = name.title() + " of " + propertyName
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Node is missing a value for a required property.",
                    runtime="WORKFLOW",
                    data=data,
                )
            if ("value" not in parameter[value]) or (parameter[value]["value"].strip() == ""):
                data["propertyName"] = value.title() + " of " + propertyName
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Node is missing a value for a required property.",
                    runtime="WORKFLOW",
                    data=data,
                )
            elif parameter[value]["widget"] in ["workflow_enum", "enum"]:
                if parameter[value]["value"] not in workflow_input_parameters and parameter[value]["value"] != "workflow.instance_name":
                    data["propertyName"] = value.title() + " of " + propertyName
                    response.add_message(
                        severity=ValidationSeverity.Error,
                        message_type="invalidNodePropertyValue",
                        message="Workflow input parameters do not contain '" + parameter[value]["value"] + "'.",
                        runtime="WORKFLOW",
                        data=data,
                    )
            

class WfpPipelineProcessorResponse(RuntimePipelineProcessorResponse):

    _type = RuntimeProcessorType.WORKFLOW_PIPELINES
    _name = "wfp"
