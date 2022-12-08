from pathlib import Path

import yaml
import json
import re


def save_dict_to_yaml(dict_value: dict, save_path: str):
    with open(save_path, "w") as file:
        file.write(yaml.dump(dict_value, allow_unicode=True))


def init_parse(init: dict):
    init_field = {

    }
    for key in init:
        if key == "init_parameters":
            init_field["parameters"] = init[key]
        elif key == "init_pipeline":
            pipeline_path = init[key]
            path_name = Path(str(pipeline_path)).stem
            init_field["pipelineTemplate"] = path_name
        else:
            pass
    return init_field


def exit_parse(exit_parameters: dict):
    exit_field = {

    }
    for key in exit_parameters:
        if key == "exit_parameters":
            exit_field["parameters"] = exit_parameters[key]
        elif key == "exit_pipeline":
            pipeline_path = exit_parameters[key]
            path_name = pipeline_path.split(".pipeline")[0].split("/")
            exit_field["pipelineTemplate"] = path_name[-1]
        else:
            pass
    return exit_field


def sepc_parse(init: dict, exit_parameters: dict, parameters: list, events: dict, triggers: dict):
    spec_field = {

    }
    if init["pipeline"]:
        spec_field["init"] = init
    if exit_parameters["pipeline"]:
        spec_field["exit"] = exit_parameters
    if len(parameters) != 0:
        spec_field["parameters"] = parameters
    if events:
        spec_field["events"] = events
    if triggers:
        spec_field["triggers"] = triggers
    return spec_field


def events_trigger_parse(node_json: dict):
    # key: catalog type
    # value: workflow.yaml field
    event_field = {}
    trigger_field = {}
    for node in node_json:
        node_type = node["op"].split(":")[0]
        if node_type == "pipeline_event":
            if "pipeline" not in event_field:
                event_field["pipeline"] = {}
            event_field["pipeline"][node["app_data"]["label"].lstrip()] = {
                "eventFilter": {
                    "expression": get_event_filter(node["app_data"]["component_parameters"]["event_filter"],
                                                   node["app_data"]["component_parameters"]["expression"])
                }
            }
        elif node_type == "model_event":
            if "model" not in event_field:
                event_field["model"] = {}
            event_field["model"][node["app_data"]["label"].lstrip()] = {
                # TODO endpoint insecure
                "eventFilter": {
                    "expression": get_event_filter(node["app_data"]["component_parameters"]["event_filter"],
                                                   node["app_data"]["component_parameters"]["expression"])
                }
            }
        elif node_type == "s3_event":
            # TODO insecure true or false?
            if "s3" not in event_field:
                event_field["s3"] = {}
            event_field["s3"][node["app_data"]["label"].lstrip()] = {
                "endpoint": node["app_data"]["component_parameters"]["endpoint"],
                "bucket": node["app_data"]["component_parameters"]["bucket"],
                "eventFilter": {
                    "expression": get_event_filter(node["app_data"]["component_parameters"]["event_filter"],
                                                   node["app_data"]["component_parameters"]["expression"])
                }
            }
        elif node_type == "calendar_event":
            if "calendar" not in event_field:
                event_field["calendar"] = {}
            event_field["calendar"][node["app_data"]["label"].lstrip()] = node["app_data"]["component_parameters"]
        elif node_type == "dataset_event":
            if "dataset" not in event_field:
                event_field["dataset"] = {}
            event_field["dataset"][node["app_data"]["label"].lstrip()] = {
                # TODO endpoint insecure
                "endpoint": "????",
                "insecure": True,
                "eventFilter": {
                    "expression": get_event_filter(node["app_data"]["component_parameters"]["event_filter"],
                                                   node["app_data"]["component_parameters"]["expression"])
                }
            }
        elif node_type == "pipeline_trigger":
            if "pipeline" not in trigger_field:
                trigger_field["pipeline"] = {}
            condition = get_condition(node_json, node)
            trigger_field["pipeline"][node["app_data"]["label"].lstrip()] = {
                "condition": condition,
                # TODO operation
                "operation": "Create",
                "pipeline": {
                    "pipelineTemplate": Path(str(node["app_data"]["component_parameters"]["template_name"])).stem,
                    # TODO parameters
                    "parameters": node["app_data"]["component_parameters"]["trigger_parameters"]
                }
            }
        elif node_type == "k8s_object_trigger":
            if "k8sobj" not in trigger_field:
                trigger_field["k8sobj"] = {}
            condition = get_condition(node_json, node)
            trigger_field["k8sobj"][node["app_data"]["label"].lstrip()] = {
                "condition": condition,
                "source": {},
                "arguments": []
            }
        elif node_type == "http_trigger":
            if "http" not in trigger_field:
                trigger_field["http"] = {}
            condition = get_condition(node_json, node)
            trigger_field["http"][node["app_data"]["label"].lstrip()] = {
                "condition": condition,
                "url": node["app_data"]["component_parameters"]["url"],
                "method": node["app_data"]["component_parameters"]["method"],
                "timeout": node["app_data"]["component_parameters"]["timeout"],
                "payload": node["app_data"]["component_parameters"]["trigger_parameters"],
            }
    return [event_field, trigger_field]


def get_condition(node_json: dict, node: dict):
    filed_map = {
        "pipeline_event": "pipeline",
        "model_event": "model",
        "minio_event": "s3",
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
            condition = "events." + filed_map[get_type(node_json, link['node_id_ref'])] + "." + \
                        get_name(node_json, link['node_id_ref'])
            condition_num += 1
        elif condition_num == 1:
            condition = "(" + condition + ")" + " && (events." + filed_map[get_type(node_json, link['node_id_ref'])] + \
                        "." + get_name(node_json, link['node_id_ref']) + ")"
        else:
            condition += " && (events." + filed_map[get_type(node_json, link['node_id_ref'])] + "." + \
                         get_name(node_json, link['node_id_ref']) + ")"

    return condition


def get_event_filter(event_filter: list[dict], expression: str):
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
                expression_item = get_expression_item(expression_id, event_filter)
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
        expression_item = get_expression_item(expression_id, event_filter)
        stack_expression += " (" + expression_item + ")"
    return stack_expression


def get_expression_item(expression_id: int, event_filter: list):
    expression_item = ""
    if expression_id in range(1, len(event_filter) + 1):
        expression_item = event_filter[expression_id - 1]["name"] + " "
        if type(event_filter[expression_id - 1]["value"]) == int:
            expression_item += event_filter[expression_id - 1]["operate"] + " " + \
                               str(event_filter[expression_id - 1]["value"])
        elif type(event_filter[expression_id - 1]["value"]) == str:
            expression_item += event_filter[expression_id - 1]["operate"] + " '" + \
                               event_filter[expression_id - 1]["value"] + "'"
    return expression_item


def get_type(node_json: dict, node_id: str):
    for node in node_json:
        if node['id'] == node_id:
            return node["op"].split(":")[0]
        else:
            continue


def get_name(node_json: dict, node_id: str):
    for node in node_json:
        if node['id'] == node_id:
            return node["app_data"]["label"].replace(' ', '')
        else:
            continue


def parameters_parse(input_parameters: list):
    return input_parameters


def validate(para: dict):
    if para:
        return True
    return False


def parse(node_json: dict):
    nodes = node_json["pipelines"][0]["nodes"]
    app_data_properties = node_json["pipelines"][0]["app_data"]["properties"]
    name = "sample"
    events = {}
    triggers = {}
    # parameters_field: list
    parameters_field = parameters_parse(app_data_properties["input_parameters"])
    # init_field: dict
    init_field = {
        "pipeline": init_parse(app_data_properties["init"])
    }
    # exit_field: dict
    exit_field = {
        "pipeline": exit_parse(app_data_properties["exit"])
    }
    events_triggers_filed = events_trigger_parse(nodes)
    spec_field = sepc_parse(init_field, exit_field, parameters_field, events_triggers_filed[0],
                            events_triggers_filed[1])
    workflow_yaml = {
        "apiVersion": "wfe.hiascend.com/v1",
        "kind": "feature",
        "metadata": {
            "name": name
        },
        "spec": spec_field
    }
    save_dict_to_yaml(workflow_yaml, "export.yaml")


if __name__ == "__main__":
    workflow_json: dict
    with open("workflow.json", "r") as f:
        workflow_json = json.load(f)
    parse(workflow_json)
