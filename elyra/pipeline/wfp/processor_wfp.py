from elyra.pipeline.processor import RuntimePipelineProcessorResponse
from elyra.pipeline.processor import RuntimePipelineProcessor
from elyra.pipeline.runtime_type import RuntimeProcessorType
from elyra.pipeline.validation import PipelineValidationManager
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
        pipeline_file = open(path, 'r', encoding='UTF-8')
        pipeline_definition = json.load(pipeline_file)
        pipeline_definition["pipelines"][0]["app_data"]["runtime_config"] = runtime_config
        pipeline_definition["pipelines"][0]["app_data"]["runtime"] = "kfp"
        pipeline_definition["pipelines"][0]["app_data"]["name"] = Path(str(path)).stem
        pipeline_definition["pipelines"][0]["app_data"]["source"] = path
        
        await PipelineValidationManager.instance().validate(pipeline_definition)
        
        pipeline = PipelineParser(root_dir=root_dir, parent=parent).parse(
            pipeline_definition
        )
        await PipelineProcessorManager.instance().export(
            pipeline, "yaml", path.replace(".pipeline", ".yaml"), True
        )
        pipeline_yaml = open(path.replace(".pipeline", ".yaml"), "r", encoding='utf-8')
        resource = pipeline_yaml.read()
        self.create_pipeline_template(path.replace(".pipeline", ".yaml"), resource, file_list)
        os.remove(path.replace(".pipeline", ".yaml"))

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
                # TODO insecure true or false?
                if "s3" not in event_field:
                    event_field["s3"] = {}
                event_field["s3"][node["app_data"]["label"].lstrip()] = {
                    "bucket": {
                        "name": self._widget_value_str(node["app_data"]["component_parameters"]["object"]["bucket_name"])
                    },
                    "eventFilter": {
                        "expression": self._get_s3_event_filter(node["app_data"]["component_parameters"]["object"],
                                                                node["app_data"]["component_parameters"]["event_filter"],
                                                                node["app_data"]["component_parameters"]["expression"])
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
                trigger_field["pipeline"][node["app_data"]["label"].lstrip()] = {
                    "condition": self._get_condition(node_json, node),
                    # TODO operation
                    "operation": node["app_data"]["component_parameters"]["operation"],
                    "pipeline": {
                        "pipelineTemplate": Path(str(node["app_data"]["component_parameters"]["template_name"])).stem,
                        # TODO parameters
                        "parameters": self._parse_trigger_parameters(
                                        node["app_data"]["component_parameters"]["trigger_parameters"],
                                        node_json)
                    }
                }
                path = node["app_data"]["component_parameters"]["template_name"]
                await self.export_pipeline(path, runtime_config, root_dir, parent, file_list)
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
                for key in node["app_data"]["component_parameters"]:
                    if key == "init_pipeline":
                        pipeline_path = node["app_data"]["component_parameters"][key]
                        path_name = Path(str(pipeline_path)).stem
                        init_pipeline_field["pipelineTemplate"] = path_name
                        await self.export_pipeline(pipeline_path, runtime_config, root_dir, parent, file_list)
                    elif key == "init_parameters":
                        init_parameters = []
                        for init_item in node["app_data"]["component_parameters"][key]:
                            temp_value = init_item["value"]["value"]
                            # if init_item["value"]["value"].isdigit():
                            #     temp_value = int(init_item["value"]["value"])
                            if "workflow.parameters" in init_item["value"]["value"]:
                                temp_value = "{{" + str(init_item["value"]["value"]) + "}}"

                            temp_init = {"name": init_item["name"], "value": temp_value,
                                         "description": init_item["description"]}
                            init_parameters.append(temp_init)
                        init_pipeline_field["parameters"] = init_parameters
                    else:
                        pass
                # pipelineTemplate first
                init_pipeline_field = dict(sorted(init_pipeline_field.items(), key=lambda x: x[0], reverse=True))
                init_field = {
                    "pipeline": init_pipeline_field
                }
            elif node_type == "exit":
                exit_pipeline_field = {}
                for key in node["app_data"]["component_parameters"]:
                    if key == "exit_pipeline":
                        pipeline_path = node["app_data"]["component_parameters"][key]
                        path_name = Path(str(pipeline_path)).stem
                        exit_pipeline_field["pipelineTemplate"] = path_name
                        await self.export_pipeline(pipeline_path, runtime_config, root_dir, parent, file_list)
                    elif key == "exit_parameters":
                        exit_parameters = []
                        for exit_item in node["app_data"]["component_parameters"][key]:
                            temp_value = exit_item["value"]["value"]
                            if exit_item["value"]["value"].isdigit():
                                temp_value = int(exit_item["value"]["value"])
                            elif "workflow.parameters" in exit_item["value"]["value"]:
                                temp_value = "{{" + str(exit_item["value"]["value"]) + "}}"

                            temp_exit = {"name": exit_item["name"], "value": temp_value,
                                         "description": exit_item["description"]}
                            exit_parameters.append(temp_exit)
                        exit_pipeline_field["parameters"] = exit_parameters
                    else:
                        pass
                # pipelineTemplate first
                exit_pipeline_field = dict(sorted(exit_pipeline_field.items(), key=lambda x: x[0], reverse=True))
                exit_field = {
                    "pipeline": exit_pipeline_field
                }
        return [init_field, event_field, trigger_field, exit_field]

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

    def _get_s3_event_filter(self, object: dict, event_filter: list[dict], expression: str):
        expression = expression.replace(' ', '')
        stack_expression = ""
        expression_id = 0
        prefix = self._widget_value_str(object["prefix"])
        suffix = self._widget_value_str(object["suffix"])
        if prefix:
            stack_expression += "object prefix '" + prefix + "'"
        if suffix:
            if stack_expression == "":
                stack_expression += "object suffix '" + suffix + "'"
            else:
                stack_expression = "(" + stack_expression + ") && " + \
                                    "(object suffix '" + suffix + "')"
        filter_expression = ""
        for char in expression:
            if char.isdigit():
                expression_id = 10 * expression_id + int(char)
            else:
                expression_item = ""
                if expression_id in range(1, len(event_filter) + 1):
                    expression_item = self._get_expression_item(expression_id, event_filter)
                if expression_item == "":
                    filter_expression += char
                else:
                    if filter_expression:
                        filter_expression += " (" + expression_item + ") " + char
                    else:
                        filter_expression += "(" + expression_item + ") " + char
                    expression_id = 0
        if expression_id != 0:
            expression_item = self._get_expression_item(expression_id, event_filter)
            if "&&" not in stack_expression and stack_expression:
                stack_expression = "(" + stack_expression + ")"
            if filter_expression == "":
                if stack_expression == "":
                    stack_expression += expression_item
                else:
                    stack_expression = stack_expression + " && (" + expression_item + ")"
            else:
                if stack_expression == "":
                    stack_expression = filter_expression + " (" + expression_item + ")"
                else:
                    stack_expression = stack_expression + " && (" + filter_expression + \
                                    " (" + expression_item + "))"
        return stack_expression

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
        # for index in range(0, len(input_parameters)):
        #     if input_parameters[index]["value"].isdigit():
        #         input_parameters[index]["value"] = int(input_parameters[index]["value"])
        return input_parameters

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
                # if item["from"]["value"].isdigit():
                #     temp_item["value"] = int(item["from"]["value"])
                # else:
                #     temp_item["value"] = item["from"]["value"]
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
    
    async def export_custom(self, root, parent, node_json: dict, export_path: str, overwrite: bool, upload: bool):
        nodes = node_json["pipelines"][0]["nodes"]
        app_data_properties = node_json["pipelines"][0]["app_data"]["properties"]
        name = Path(str(export_path)).stem
        parameters_field = []
        file_list = []
        runtime_config = node_json["pipelines"][0]["app_data"]["runtime_config"]
        if app_data_properties.__contains__("input_parameters"):
            parameters_field = self._parameters_parse(app_data_properties["input_parameters"])
        para_filed = await self._component_parse(root, parent, nodes, runtime_config, export_path, file_list)
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
        runtime_configuration = self._get_metadata_configuration(
            schemaspace=Runtimes.RUNTIMES_SCHEMASPACE_ID, name=runtime_config
        )
        api_endpoint = runtime_configuration.metadata.get("api_endpoint")
        description = ""
        if "description" in node_json["pipelines"][0]["app_data"]["properties"]:
            description = node_json["pipelines"][0]["app_data"]["properties"]["description"]
        if upload:
            await self.upload(zip_file_name, api_endpoint, name, description)
        return zip_file_name
    
    async def upload(self, filePath: str, api_endpoint: str, name: str, description: str):
        url = api_endpoint + "/apis/v1beta1/workflows/upload"
        files = {'uploadfile': open(filePath, 'rb')}
        values = {'name': name, 'description': description}
        result = requests.post(url, files=files, params=values)
        print(result)

class WfpPipelineProcessorResponse(RuntimePipelineProcessorResponse):

    _type = RuntimeProcessorType.WORKFLOW_PIPELINES
    _name = "wfp"
