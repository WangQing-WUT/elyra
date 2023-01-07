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
        spec_field = {

        }
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
    
    def _get_dataset_names(self, dataset_names: dict):
        dataset_names_list = []
        for item in dataset_names:
            value = self._widget_value_str(item)
            if value:
                dataset_names_list.append(value)
        return dataset_names_list

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
            if node_type == "pipeline_event":
                if "pipeline" not in event_field:
                    event_field["pipeline"] = {}
                event_field["pipeline"][node["app_data"]["label"].lstrip()] = {
                    "eventFilter": {
                        "expression": self._get_event_filter(node["app_data"]["component_parameters"]["event_filter"],
                                                             node["app_data"]["component_parameters"]["expression"])
                    }
                }
            elif node_type == "model_event":
                if "model" not in event_field:
                    event_field["model"] = {}
                event_field["model"][node["app_data"]["label"].lstrip()] = {
                    "eventFilter": {
                        "expression": self._get_event_filter(node["app_data"]["component_parameters"]["event_filter"],
                                                             node["app_data"]["component_parameters"]["expression"])
                    }
                }
            elif node_type == "s3_event":
                if "s3" not in event_field:
                    event_field["s3"] = {}
                event_field["s3"][node["app_data"]["label"].lstrip()] = {
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
                event_field["calendar"][node["app_data"]["label"].lstrip()] = self._get_calendar_data(node["app_data"]["component_parameters"]["calendar"])
            elif node_type == "dataset_event":
                if "dataset" not in event_field:
                    event_field["dataset"] = {}
                event_field["dataset"][node["app_data"]["label"].lstrip()] = {
                    "datasets": self._get_dataset_names(node["app_data"]["component_parameters"]["dataset_name"]),
                    "eventFilter": {
                        "expression": self._get_event_filter(node["app_data"]["component_parameters"]["event_filter"],
                                                             node["app_data"]["component_parameters"]["expression"])
                    }
                }
            elif node_type == "pipeline_trigger":
                if "pipeline" not in trigger_field:
                    trigger_field["pipeline"] = {}
                parameters = self._parse_trigger_parameters(
                                node["app_data"]["component_parameters"]["trigger_parameters"],
                                node_json
                            )
                path = node["app_data"]["component_parameters"]["template_name"]
                response, pipeline_input_parameters = await self.export_pipeline(path, runtime_config, root_dir, parent, file_list)
                if response.has_fatal:
                    response.update_message(data)
                    return [init_field, event_field, trigger_field, exit_field, response]
                for parameter in parameters:
                    for pipeline_input_parameter in pipeline_input_parameters:
                        if pipeline_input_parameter["name"] == parameter["name"]:
                            pipeline_input_parameters.remove(pipeline_input_parameter)
                            break
                parameters += pipeline_input_parameters
                trigger_field["pipeline"][node["app_data"]["label"].lstrip()] = {
                    "condition": self._get_condition(node_json, node),
                    "operation": "create",
                    "pipeline": {
                        "pipelineTemplate": Path(str(node["app_data"]["component_parameters"]["template_name"])).stem,
                        "parameters": parameters
                    }
                }
                data = {
                    "workflowSource": "Pipeline Trigger",
                    "pipelineTriggerName": node["app_data"]["label"].lstrip()
                }
            elif node_type == "k8s_object_trigger":
                if "k8sobj" not in trigger_field:
                    trigger_field["k8sobj"] = {}
                trigger_field["k8sobj"][node["app_data"]["label"].lstrip()] = {
                    "condition": self._get_condition(node_json, node),
                    "source": self._get_k8s_source(node["app_data"]["component_parameters"]["source"]),
                    "arguments": self._get_k8s_agruments(
                        node["app_data"]["component_parameters"]["trigger_parameters"], node_json)
                }
            elif node_type == "http_trigger":
                if "http" not in trigger_field:
                    trigger_field["http"] = {}
                trigger_field["http"][node["app_data"]["label"].lstrip()] = {
                    "condition": self._get_condition(node_json, node),
                    "url": node["app_data"]["component_parameters"]["url"],
                    "method": node["app_data"]["component_parameters"]["method"],
                    "timeout": node["app_data"]["component_parameters"]["timeout"],
                    "payload": self._parse_trigger_parameters(
                                        node["app_data"]["component_parameters"]["trigger_parameters"],
                                        node_json),
                }
            elif node_type == "init":
                init_pipeline_field = {}
                pipeline_input_parameters = {}

                pipeline_path = node["app_data"]["component_parameters"]["init_pipeline"]
                path_name = Path(str(pipeline_path)).stem
                init_pipeline_field["pipelineTemplate"] = path_name
                response, pipeline_input_parameters = await self.export_pipeline(pipeline_path, runtime_config, root_dir, parent, file_list)
                data = {
                    "workflowSource": "Init"
                }
                if response.has_fatal:
                    response.update_message(data)
                    return [init_field, event_field, trigger_field, exit_field, response]

                init_parameters = []
                for init_item in node["app_data"]["component_parameters"]["init_parameters"]:
                    temp_value = init_item["value"]["value"]
                    if "workflow.parameters" in init_item["value"]["value"]:
                        temp_value = "{{" + str(init_item["value"]["value"]) + "}}"

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

                # pipelineTemplate first
                init_pipeline_field = dict(sorted(init_pipeline_field.items(), key=lambda x: x[0], reverse=True))
                init_field = {
                    "pipeline": init_pipeline_field
                }
            elif node_type == "exit":
                exit_pipeline_field = {}
                pipeline_input_parameters = {}

                pipeline_path = node["app_data"]["component_parameters"]["exit_pipeline"]
                path_name = Path(str(pipeline_path)).stem
                exit_pipeline_field["pipelineTemplate"] = path_name
                response, pipeline_input_parameters = await self.export_pipeline(pipeline_path, runtime_config, root_dir, parent, file_list)
                data = {
                    "workflowSource": "Exit"
                }
                if response.has_fatal:
                    response.update_message(data)
                    return [init_field, event_field, trigger_field, exit_field, response]

                exit_parameters = []
                for exit_item in node["app_data"]["component_parameters"]["exit_parameters"]:
                    temp_value = exit_item["value"]["value"]
                    if exit_item["value"]["value"].isdigit():
                        temp_value = int(exit_item["value"]["value"])
                    elif "workflow.parameters" in exit_item["value"]["value"]:
                        temp_value = "{{" + str(exit_item["value"]["value"]) + "}}"

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

                # pipelineTemplate first
                exit_pipeline_field = dict(sorted(exit_pipeline_field.items(), key=lambda x: x[0], reverse=True))
                exit_field = {
                    "pipeline": exit_pipeline_field
                }
        return [init_field, event_field, trigger_field, exit_field, response]

    def _get_condition(self, node_json: dict, node: dict):
        filed_map = {
            "pipeline_event": "pipeline",
            "model_event": "model",
            "s3_event": "s3",
            "calendar_event": "calendar",
            "dataset_event": "dataset",
            "pipeline_trigger": "pipeline",
            "k8s_object_trigger": "k8sobj",
            "http_trigger": "http"
        }
        condition = ""
        condition_num = 0
        for link in node["inputs"][0]["links"]:
            if condition_num == 0:
                condition = "events." + filed_map[self._get_type(node_json, link['node_id_ref'])] + "." + \
                            self._get_name(node_json, link['node_id_ref'])
                condition_num += 1
            elif condition_num == 1:
                condition = "(" + condition + ")" + " && (events." + filed_map[
                    self._get_type(node_json, link['node_id_ref'])] + \
                            "." + self._get_name(node_json, link['node_id_ref']) + ")"
                condition_num += 1
            else:
                condition += " && (events." + filed_map[self._get_type(node_json, link['node_id_ref'])] + "." + \
                             self._get_name(node_json, link['node_id_ref']) + ")"
        return condition

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
        if dict["value"] == "":
            return ""
        elif dict["widget"] == "string":
            return dict["value"]
        elif dict["widget"] == "enum":
            return "{{" + dict["value"] + "}}"
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
                return node["app_data"]["label"].replace(' ', '')
            else:
                continue

    @staticmethod
    def _parameters_parse(input_parameters: list):
        format_input_parameters = []
        for item in input_parameters:
            input_parameter = {}
            input_parameter["name"] = item["name"]
            input_parameter["type"] = item["type"]
            if "value" in item:
                input_parameter["value"] = item["value"]
            if "description" in item:
                input_parameter["description"] = item["description"]
            format_input_parameters.append(input_parameter)
        return format_input_parameters

    @staticmethod
    def _parse_trigger_parameters(trigger_parameters: dict, node_json: dict):
        field_map = {
            "pipeline_event": "pipeline",
            "model_event": "model",
            "s3_event": "s3",
            "calendar_event": "calendar",
            "dataset_event": "dataset",
            "pipeline_trigger": "pipeline",
            "k8s_object_trigger": "k8sobj",
            "http_trigger": "http",
            "init": "init",
            "exit": "exit"
        }

        for node in node_json:
            field_map[node["app_data"]["label"]] = field_map[node["op"].split(":")[0]]
        trigger_parameters_field = []
        for item in trigger_parameters:
            temp_item = {"name": item["name"]}
            if item["from"]["widget"] == "enum":
                if "workflow.parameters" in item["from"]["value"]:
                    temp_item["value"] = "{{" + item["from"]["value"] + "}}"
                else:
                    name = item["from"]["value"].split(":")[0]
                    temp_item["value"] = "{{events." + field_map[name] + "." + item["from"]["value"] + "}}"
                    temp_item["value"] = temp_item["value"].replace(": ", ".")
            else:
                temp_item["value"] = item["from"]["value"]
            trigger_parameters_field.append(temp_item)
        return trigger_parameters_field
    
    @staticmethod
    def _get_calendar_data(parameters: dict):
        data_field = {}
        if parameters["name"] == "interval":
            if parameters["value"]["widget"] == "enum":
                data_field["interval"] = "{{" + parameters["value"]["value"] + "}}"
            elif parameters["value"]["widget"] == "string":
                data_field["interval"] = parameters["value"]["value"]
        elif parameters["name"] == "schedule":
            if parameters["value"]["widget"] == "enum":
                data_field["schedule"] = "{{" + parameters["value"]["value"] + "}}"
            elif parameters["value"]["widget"] == "string":
                data_field["schedule"] = parameters["value"]["value"]
        return data_field

    @staticmethod
    def _get_k8s_agruments(parameters: list, node_json: dict):
        field_map = {
            "pipeline_event": "pipeline",
            "model_event": "model",
            "s3_event": "s3",
            "calendar_event": "calendar",
            "dataset_event": "dataset",
            "pipeline_trigger": "pipeline",
            "k8s_object_trigger": "k8sobj",
            "http_trigger": "http",
            "init": "init",
            "exit": "exit"
        }
        for node in node_json:
            field_map[node["app_data"]["label"]] = field_map[node["op"].split(":")[0]]
        arguments_list = []
        for dict_item in parameters:
            temp_argument = {}
            if dict_item["from"]["widget"] == "string":
                temp_argument["src"] = dict_item["from"]["value"]
            elif dict_item["from"]["widget"] == "enum":
                if "workflow.parameters" in dict_item["from"]["value"]:
                    temp_argument["src"] = "{{" + dict_item["from"]["value"] + "}}"
                else:
                    name = dict_item["from"]["value"].split(":")[0]
                    temp_argument["src"] = "{{events." + field_map[name] + "." + dict_item["from"]["value"] + "}}"
                    temp_argument["src"] = temp_argument["src"].replace(": ", ".")
            temp_argument["dest"] = "{{" + str(dict_item["dest"]) + "}}"
            arguments_list.append(temp_argument)
        return arguments_list

    @staticmethod
    def _get_k8s_source(parameters: dict):
        if parameters["widget"] == "s3":
            return {
                "s3": {
                    "bucket": {
                        "name": parameters["bucket_name"]
                    },
                    "object": parameters["object"]
                }
            }
        elif parameters["widget"] == "http":
            return {
                "http": {
                    "url": parameters["url"]
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
            para_filed = await self._component_parse(root, parent, nodes, runtime_config, export_path, file_list)
            response = para_filed[4]
            if not response.has_fatal:
                spec_field = self._sepc_parse(para_filed[0], para_filed[3], parameters_field, para_filed[1],
                                            para_filed[2])
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
        for parameter in input_parameters:
            if ("name" not in parameter) or (parameter["name"].strip() == "") :
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodeProperty",
                    message="The 'Parameter Name' field of workflow input parameters cannot be empty.",
                    runtime="WORKFLOW",
                )
            else:
                workflow_input_parameters.append("workflow.parameters." + parameter["name"])
        for node in nodes:
            node_type = node["op"].split(":")[0]
            if node_type == "pipeline_event":
                self._validate_pipeline_event(node, workflow_input_parameters, name, response)
            elif node_type == "model_event":
                self._validate_model_event(node, workflow_input_parameters, name, response)
            elif node_type == "s3_event":
                self._validate_s3_event(node, workflow_input_parameters, name, response)
            elif node_type == "calendar_event":
                self._validate_calendar_event(node, workflow_input_parameters, name, response)
            elif node_type == "dataset_event":
                self._validate_dataset_event(node, workflow_input_parameters, name, response)
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

    def _validate_pipeline_event(self, node, workflow_input_parameters, name, response):
        pass

    def _validate_model_event(self, node, workflow_input_parameters, name, response):
        pass
    
    def _validate_s3_event(self, node, workflow_input_parameters, name, response):
        node_type = "S3 Event"
        node_id = node["id"]
        node_name = node["app_data"]["label"]
        self._validate_node_name(node, name, response)
        bucket_name = node["app_data"]["component_parameters"]["bucket_name"]
        s3_object_prefix = node["app_data"]["component_parameters"]["prefix"]
        s3_object_suffix = node["app_data"]["component_parameters"]["suffix"]
        self._validate_node_property_value(bucket_name, node_type, node_id, node_name, "Bucket Name", workflow_input_parameters, response)
        if s3_object_prefix["widget"] == "enum":
            if s3_object_prefix["value"] not in workflow_input_parameters:
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Workflow input parameters do not contain '" + s3_object_prefix["value"] + "'.",
                    runtime="WORKFLOW",
                    data={
                        "nodeType": node_type,
                        "nodeID": node_id,
                        "nodeName": node_name, 
                        "propertyName": "S3 Object Prefix"
                    },
                )
        if s3_object_suffix["widget"] == "enum":
            if s3_object_suffix["value"] not in workflow_input_parameters:
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Workflow input parameters do not contain '" + s3_object_suffix["value"] + "'.",
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
            for event_filter in event_filters:
                self._validate_event_filter(event_filter, node_type, node_id, node_name, "Dataset Event Filter", workflow_input_parameters, response)

    
    def _validate_calendar_event(self, node, workflow_input_parameters, name, response):
        node_type = "Calendar Event"
        node_id = node["id"]
        node_name = node["app_data"]["label"]
        self._validate_node_name(node, name, response)
        calendar = node["app_data"]["component_parameters"]["calendar"]
        self._validate_node_property_value(calendar["value"], node_type, node_id, node_name, calendar["name"], workflow_input_parameters, response)
    
    def _validate_dataset_event(self, node, workflow_input_parameters, name, response):
        node_type = "Dataset Event"
        node_id = node["id"]
        node_name = node["app_data"]["label"]
        self._validate_node_name(node, name, response)
        if ("dataset_name" not in node["app_data"]["component_parameters"]) or (len(node["app_data"]["component_parameters"]["dataset_name"]) == 0):
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data={
                    "nodeType": node_type,
                    "nodeID": node_id,
                    "nodeName": node_name, 
                    "propertyName": "Dataset Name"
                },
            )
        else:
            dataset_names = node["app_data"]["component_parameters"]["dataset_name"]
            for dataset_name in dataset_names:
                self._validate_node_property_value(dataset_name, node_type, node_id, node_name, "Dataset Name", workflow_input_parameters, response)

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
            for event_filter in event_filters:
                self._validate_event_filter(event_filter, node_type, node_id, node_name, "Dataset Event Filter", workflow_input_parameters, response)

    
    def _validate_pipeline_trigger(self, node, workflow_input_parameters, name, response):
        node_type = "Pipeline Trigger"
        node_id = node["id"]
        node_name = node["app_data"]["label"]
        self._validate_node_name(node, name, response)
        self._validate_node_links(node, node_type, node_id, node_name, response)
        if "trigger_parameters" not in node["app_data"]["component_parameters"]:
            node["app_data"]["component_parameters"]["trigger_parameters"] = []
        self._validate_trigger_from()
    
    def _validate_k8s_object_trigger(self, node, workflow_input_parameters, name, response):
        pass
    
    def _validate_http_trigger(self, node, workflow_input_parameters, name, response):
        pass
    
    def _validate_init(self, node, workflow_input_parameters, name, response):
        node_type = "Init"
        node_id = node["id"]
        node_name = node["app_data"]["label"]
        if "init_parameters" not in node["app_data"]["component_parameters"]:
            node["app_data"]["component_parameters"]["init_parameters"] = []
        init_parameters = node["app_data"]["component_parameters"]["init_parameters"]
        self._validate_init_exit(init_parameters, node_type, node_id, node_name, "Init Parameters", workflow_input_parameters, response)
    
    def _validate_exit(self, node, workflow_input_parameters, name, response):
        node_type = "Exit"
        node_id = node["id"]
        node_name = node["app_data"]["label"]
        if "exit_parameters" not in node["app_data"]["component_parameters"]:
            node["app_data"]["component_parameters"]["exit_parameters"] = []
        exit_parameters = node["app_data"]["component_parameters"]["exit_parameters"]
        self._validate_init_exit(exit_parameters, node_type, node_id, node_name, "Exit Parameters", workflow_input_parameters, response)
    
    def _validate_node_name(self, node, name, response):
        if node["app_data"]["label"] in name:
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodeName",
                message="Workflow component name fields cannot be the same",
                runtime="WORKFLOW",
                data={
                    "duplicateName": node["app_data"]["label"],
                    "node1": name[node["app_data"]["label"]],
                    "node2": node["id"]
                }
            )
        else:
            name[node["app_data"]["label"]] = node["id"]
    
    def _validate_node_property_value(self, property, nodeType, nodeID, nodeName, propertyName, workflow_input_parameters, response):        
        if property["value"].strip() == "":
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data={
                    "nodeType": nodeType,
                    "nodeID": nodeID,
                    "nodeName": nodeName, 
                    "propertyName": propertyName
                },
            )
        elif property["widget"] == "enum":
            if property["value"] not in workflow_input_parameters:
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Workflow input parameters do not contain '" + property["value"] + "'.",
                    runtime="WORKFLOW",
                    data={
                        "nodeType": nodeType,
                        "nodeID": nodeID,
                        "nodeName": nodeName, 
                        "propertyName": propertyName
                    },
                )
    
    def _validate_event_filter(self, property, nodeType, nodeID, nodeName, propertyName, workflow_input_parameters, response):
        if "name" not in property:
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data={
                    "nodeType": nodeType,
                    "nodeID": nodeID,
                    "nodeName": nodeName, 
                    "propertyName": "Property Name of " + propertyName,
                    "filterID": property["id"]
                },
            )
        if "operate" not in property:
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data={
                    "nodeType": nodeType,
                    "nodeID": nodeID,
                    "nodeName": nodeName, 
                    "propertyName": "Operate of " + propertyName,
                    "filterID": property["id"]
                },
            )
        if property["value"]["value"] == "":
            response.add_message(
                severity=ValidationSeverity.Error,
                message_type="invalidNodePropertyValue",
                message="Node is missing a value for a required property.",
                runtime="WORKFLOW",
                data={
                    "nodeType": nodeType,
                    "nodeID": nodeID,
                    "nodeName": nodeName, 
                    "propertyName": "Value of " + propertyName,
                    "filterID": property["id"]
                },
            )
        elif property["value"]["widget"] == "enum":
            if property["value"]["value"] not in workflow_input_parameters:
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Workflow input parameters do not contain '" + property["value"]["value"] + "'.",
                    runtime="WORKFLOW",
                    data={
                        "nodeType": nodeType,
                        "nodeID": nodeID,
                        "nodeName": nodeName, 
                        "propertyName": "Value of " + propertyName,
                        "filterID": property["id"]
                    },
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

    def _validate_trigger_from(self):
        pass

    def _validate_init_exit(self, parameters, nodeType, nodeID, nodeName, propertyName, workflow_input_parameters, response):
        for parameter in parameters:
            if "name" not in parameter:
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Node is missing a value for a required property.",
                    runtime="WORKFLOW",
                    data={
                        "nodeType": nodeType,
                        "nodeID": nodeID,
                        "nodeName": nodeName,
                        "propertyName": "Name of " + propertyName,
                    },
                )
            if parameter["value"]["value"] == "":
                response.add_message(
                    severity=ValidationSeverity.Error,
                    message_type="invalidNodePropertyValue",
                    message="Node is missing a value for a required property.",
                    runtime="WORKFLOW",
                    data={
                        "nodeType": nodeType,
                        "nodeID": nodeID,
                        "nodeName": nodeName, 
                        "propertyName": "Value of " + propertyName,
                    },
                )
            if parameter["value"]["widget"] == "enum":
                if parameter["value"]["value"] not in workflow_input_parameters:
                    response.add_message(
                        severity=ValidationSeverity.Error,
                        message_type="invalidNodePropertyValue",
                        message="Workflow input parameters do not contain '" + parameters["value"]["value"] + "'.",
                        runtime="WORKFLOW",
                        data={
                            "nodeType": nodeType,
                            "nodeID": nodeID,
                            "nodeName": nodeName, 
                            "propertyName": "Value of " + propertyName,
                        },
                    )
            

class WfpPipelineProcessorResponse(RuntimePipelineProcessorResponse):

    _type = RuntimeProcessorType.WORKFLOW_PIPELINES
    _name = "wfp"
