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
from datetime import datetime
import inspect
import operator
import os
from pathlib import Path
import re
import tempfile
import time
from typing import Any
from typing import Dict
from typing import Set
from urllib.parse import urlsplit

from kfp import Client as ArgoClient
from kfp import compiler as kfp_argo_compiler
from kfp import components as components
import kfp.dsl as dsl
from kfp.dsl import PipelineConf
from kfp.aws import use_aws_secret  # noqa H306
from kubernetes import client as k8s_client
from kubernetes.client import V1EnvVar
from kubernetes.client import V1EnvVarSource
from kubernetes.client import V1PersistentVolumeClaimVolumeSource
from kubernetes.client import V1SecretKeySelector
from kubernetes.client import V1Toleration
from kubernetes.client import V1Volume
from kubernetes.client import V1VolumeMount

try:
    from kfp_tekton import compiler as kfp_tekton_compiler
    from kfp_tekton import TektonClient
except ImportError:
    # We may not have kfp-tekton available and that's okay!
    kfp_tekton_compiler = None
    TektonClient = None

from elyra._version import __version__
from elyra.kfp.operator import ExecuteFileOp
from elyra.metadata.schemaspaces import RuntimeImages
from elyra.metadata.schemaspaces import Runtimes
from elyra.pipeline import pipeline_constants
from elyra.pipeline.component_catalog import ComponentCache
from elyra.pipeline.component_parameter import DisableNodeCaching
from elyra.pipeline.component_parameter import ElyraProperty
from elyra.pipeline.component_parameter import ElyraPropertyList
from elyra.pipeline.component_parameter import EnvironmentVariable
from elyra.pipeline.component_parameter import KubernetesAnnotation
from elyra.pipeline.component_parameter import KubernetesLabel
from elyra.pipeline.component_parameter import KubernetesSecret
from elyra.pipeline.component_parameter import KubernetesToleration
from elyra.pipeline.component_parameter import VolumeMount
from elyra.pipeline.kfp.kfp_authentication import AuthenticationError
from elyra.pipeline.kfp.kfp_authentication import KFPAuthenticator
from elyra.pipeline.pipeline import GenericOperation
from elyra.pipeline.pipeline import Operation
from elyra.pipeline.pipeline import Pipeline
from elyra.pipeline.processor import PipelineProcessor
from elyra.pipeline.processor import RuntimePipelineProcessor
from elyra.pipeline.processor import RuntimePipelineProcessorResponse
from elyra.pipeline.runtime_type import RuntimeProcessorType
from elyra.util.cos import join_paths
from elyra.util.path import get_absolute_path

loop_stack = []
global_loop_args = {}


class KfpPipelineProcessor(RuntimePipelineProcessor):
    _type = RuntimeProcessorType.KUBEFLOW_PIPELINES
    _name = "kfp"

    # Provide users with the ability to identify a writable directory in the
    # running container where the notebook | script is executed. The location
    # must exist and be known before the container is started.
    # Defaults to `/tmp`
    WCD = os.getenv("ELYRA_WRITABLE_CONTAINER_DIR", "/tmp").strip().rstrip("/")

    def process(self, pipeline):
        """
        Runs a pipeline on Kubeflow Pipelines

        Each time a pipeline is processed, a new version
        is uploaded and run under the same experiment name.
        """

        timestamp = datetime.now().strftime("%m%d%H%M%S")

        ################
        # Runtime Configs
        ################
        runtime_configuration = self._get_metadata_configuration(
            schemaspace=Runtimes.RUNTIMES_SCHEMASPACE_ID,
            name=pipeline.runtime_config,
        )

        # unpack Kubeflow Pipelines configs
        api_endpoint = runtime_configuration.metadata.get("api_endpoint").rstrip("/")
        public_api_endpoint = runtime_configuration.metadata.get("public_api_endpoint", api_endpoint)
        api_username = runtime_configuration.metadata.get("api_username")
        api_password = runtime_configuration.metadata.get("api_password")
        user_namespace = runtime_configuration.metadata.get("user_namespace")
        engine = runtime_configuration.metadata.get("engine")
        if engine == "Tekton" and not TektonClient:
            raise ValueError(
                "Python package `kfp-tekton` is not installed. "
                "Please install using `elyra[kfp-tekton]` to use Tekton engine."
            )

        # unpack Cloud Object Storage configs
        cos_endpoint = runtime_configuration.metadata.get("cos_endpoint")
        cos_public_endpoint = runtime_configuration.metadata.get("public_cos_endpoint", cos_endpoint)
        cos_bucket = runtime_configuration.metadata.get("cos_bucket")

        # Determine which provider to use to authenticate with Kubeflow
        auth_type = runtime_configuration.metadata.get("auth_type")

        try:
            auth_info = KFPAuthenticator().authenticate(
                api_endpoint,
                auth_type_str=auth_type,
                runtime_config_name=pipeline.runtime_config,
                auth_parm_1=api_username,
                auth_parm_2=api_password,
            )
            self.log.debug(f"Authenticator returned {auth_info}")
        except AuthenticationError as ae:
            if ae.get_request_history() is not None:
                self.log.info("An authentication error was raised. Diagnostic information follows.")
                self.log.info(ae.request_history_to_string())
            raise RuntimeError(f"Kubeflow authentication failed: {ae}")

        #############
        # Create Kubeflow Client
        #############
        try:
            if engine == "Tekton":
                client = TektonClient(
                    host=api_endpoint,
                    cookies=auth_info.get("cookies", None),
                    credentials=auth_info.get("credentials", None),
                    existing_token=auth_info.get("existing_token", None),
                    namespace=user_namespace,
                )
            else:
                client = ArgoClient(
                    host=api_endpoint,
                    cookies=auth_info.get("cookies", None),
                    credentials=auth_info.get("credentials", None),
                    existing_token=auth_info.get("existing_token", None),
                    namespace=user_namespace,
                )
        except Exception as ex:
            # a common cause of these errors is forgetting to include `/pipeline` or including it with an 's'
            api_endpoint_obj = urlsplit(api_endpoint)
            if api_endpoint_obj.path != "/pipeline":
                api_endpoint_tip = api_endpoint_obj._replace(path="/pipeline").geturl()
                tip_string = (
                    f" - [TIP: did you mean to set '{api_endpoint_tip}' as the endpoint, "
                    f"take care not to include 's' at end]"
                )
            else:
                tip_string = ""

            raise RuntimeError(
                f"Failed to initialize `kfp.Client()` against: '{api_endpoint}' - "
                f"Check Kubeflow Pipelines runtime configuration: '{pipeline.runtime_config}'"
                f"{tip_string}"
            ) from ex

        #############
        # Verify Namespace
        #############
        try:
            client.list_experiments(namespace=user_namespace, page_size=1)
        except Exception as ex:
            if user_namespace:
                tip_string = f"[TIP: ensure namespace '{user_namespace}' is correct]"
            else:
                tip_string = "[TIP: you probably need to set a namespace]"

            raise RuntimeError(
                f"Failed to `kfp.Client().list_experiments()` against: '{api_endpoint}' - "
                f"Check Kubeflow Pipelines runtime configuration: '{pipeline.runtime_config}' - "
                f"{tip_string}"
            ) from ex

        #############
        # Pipeline Metadata none - inherited
        #############
        # generate a pipeline name
        pipeline_name = pipeline.name

        # generate a pipeline description
        pipeline_description = pipeline.description
        if pipeline_description is None:
            pipeline_description = f"Created with Elyra {__version__} pipeline editor using `{pipeline.source}`."

        #############
        # Submit & Run the Pipeline
        #############
        self.log_pipeline_info(pipeline_name, "submitting pipeline")

        with tempfile.TemporaryDirectory() as temp_dir:
            self.log.debug(f"Created temporary directory at: {temp_dir}")
            pipeline_path = os.path.join(temp_dir, f"{pipeline_name}.tar.gz")

            #############
            # Get Pipeline ID
            #############
            try:
                # get the kubeflow pipeline id (returns None if not found, otherwise the ID of the pipeline)
                pipeline_id = client.get_pipeline_id(pipeline_name)

                # calculate what "pipeline version" name to use
                if pipeline_id is None:
                    # the first "pipeline version" name must be the pipeline name
                    pipeline_version_name = pipeline_name
                else:
                    # generate a unique name for a new "pipeline version" by appending the current timestamp
                    pipeline_version_name = f"{pipeline_name}-{timestamp}"

            except Exception as ex:
                raise RuntimeError(
                    f"Failed to get ID of Kubeflow pipeline: '{pipeline_name}' - "
                    f"Check Kubeflow Pipelines runtime configuration: '{pipeline.runtime_config}'"
                ) from ex

            #############
            # Compile the Pipeline
            #############
            try:
                t0 = time.time()

                # generate a name for the experiment (lowercase because experiments are case intensive)
                experiment_name = pipeline_name.lower()

                # Create an instance id that will be used to store
                # the pipelines' dependencies, if applicable
                pipeline_instance_id = f"{pipeline_name}-{timestamp}"

                def gen_function(args):
                    def new_function(*args, **kwargs):
                        self._cc_pipeline(
                            pipeline,
                            pipeline_name,
                            pipeline_instance_id=pipeline_instance_id,
                            args=args,
                        )

                    params = [
                        inspect.Parameter(
                            param,
                            inspect.Parameter.POSITIONAL_OR_KEYWORD,
                            annotation=type(value),
                            default=value,
                        )
                        for param, value in args.items()
                    ]
                    new_function.__signature__ = inspect.Signature(params)
                    new_function.__annotations__ = args
                    name = Path(str(pipeline_name)).stem
                    new_function.__name__ = name
                    return new_function

                # collect pipeline configuration information
                pipeline_conf = self._generate_pipeline_conf(pipeline)

                input_parameters = self._get_pipeline_input_parameters(pipeline)
                pipeline_function = gen_function(input_parameters)

                # compile the pipeline
                if engine == "Tekton":
                    kfp_tekton_compiler.TektonCompiler().compile(
                        pipeline_function,
                        pipeline_path,
                        pipeline_conf=pipeline_conf,
                    )
                else:
                    kfp_argo_compiler.Compiler().compile(
                        pipeline_function,
                        pipeline_path,
                        pipeline_conf=pipeline_conf,
                    )
            except RuntimeError:
                raise
            except Exception as ex:
                raise RuntimeError(
                    f"Failed to compile pipeline '{pipeline_name}' with engine '{engine}' to: '{pipeline_path}'"
                ) from ex

            self.log_pipeline_info(pipeline_name, "pipeline compiled", duration=time.time() - t0)

            #############
            # Upload Pipeline Version
            #############
            try:
                t0 = time.time()

                # CASE 1: pipeline needs to be created
                if pipeline_id is None:
                    # create new pipeline (and initial "pipeline version")
                    kfp_pipeline = client.upload_pipeline(
                        pipeline_package_path=pipeline_path,
                        pipeline_name=pipeline_name,
                        description=pipeline_description,
                    )

                    # extract the ID of the pipeline we created
                    pipeline_id = kfp_pipeline.id

                    # the initial "pipeline version" has the same id as the pipeline itself
                    version_id = pipeline_id

                # CASE 2: pipeline already exists
                else:
                    # upload the "pipeline version"
                    kfp_pipeline = client.upload_pipeline_version(
                        pipeline_package_path=pipeline_path,
                        pipeline_version_name=pipeline_version_name,
                        pipeline_id=pipeline_id,
                    )

                    # extract the id of the "pipeline version" that was created
                    version_id = kfp_pipeline.id

            except Exception as ex:
                # a common cause of these errors is forgetting to include `/pipeline` or including it with an 's'
                api_endpoint_obj = urlsplit(api_endpoint)
                if api_endpoint_obj.path != "/pipeline":
                    api_endpoint_tip = api_endpoint_obj._replace(path="/pipeline").geturl()
                    tip_string = (
                        f" - [TIP: did you mean to set '{api_endpoint_tip}' as the endpoint, "
                        f"take care not to include 's' at end]"
                    )
                else:
                    tip_string = ""

                raise RuntimeError(
                    f"Failed to upload Kubeflow pipeline '{pipeline_name}' - "
                    f"Check Kubeflow Pipelines runtime configuration: '{pipeline.runtime_config}'"
                    f"{tip_string}"
                ) from ex

            self.log_pipeline_info(pipeline_name, "pipeline uploaded", duration=time.time() - t0)

            #############
            # Create Experiment
            #############
            try:
                t0 = time.time()

                # create a new experiment (if already exists, this a no-op)
                experiment = client.create_experiment(name=experiment_name, namespace=user_namespace)

            except Exception as ex:
                raise RuntimeError(
                    f"Failed to create Kubeflow experiment: '{experiment_name}' - "
                    f"Check Kubeflow Pipelines runtime configuration: '{pipeline.runtime_config}'"
                ) from ex

            self.log_pipeline_info(pipeline_name, "created experiment", duration=time.time() - t0)

            #############
            # Create Pipeline Run
            #############
            try:
                t0 = time.time()

                # generate name for the pipeline run
                job_name = pipeline_instance_id

                # create pipeline run (or specified pipeline version)
                run = client.run_pipeline(
                    experiment_id=experiment.id,
                    job_name=job_name,
                    pipeline_id=pipeline_id,
                    version_id=version_id,
                )

            except Exception as ex:
                raise RuntimeError(
                    f"Failed to create Kubeflow pipeline run: '{job_name}' - "
                    f"Check Kubeflow Pipelines runtime configuration: '{pipeline.runtime_config}'"
                ) from ex

            if run is None:
                # client.run_pipeline seemed to have encountered an issue
                # but didn't raise an exception
                raise RuntimeError(
                    f"Failed to create Kubeflow pipeline run: '{job_name}' - "
                    f"Check Kubeflow Pipelines runtime configuration: '{pipeline.runtime_config}'"
                )

            self.log_pipeline_info(
                pipeline_name,
                f"pipeline submitted: {public_api_endpoint}/#/runs/details/{run.id}",
                duration=time.time() - t0,
            )

        if pipeline.contains_generic_operations():
            object_storage_url = f"{cos_public_endpoint}"
            os_path = join_paths(
                pipeline.pipeline_parameters.get(pipeline_constants.COS_OBJECT_PREFIX),
                pipeline_instance_id,
            )
            object_storage_path = f"/{cos_bucket}/{os_path}"
        else:
            object_storage_url = None
            object_storage_path = None

        return KfpPipelineProcessorResponse(
            run_id=run.id,
            run_url=f"{public_api_endpoint}/#/runs/details/{run.id}",
            object_storage_url=object_storage_url,
            object_storage_path=object_storage_path,
        )

    def export(
        self,
        pipeline: Pipeline,
        pipeline_export_format: str,
        pipeline_export_path: str,
        overwrite: bool,
    ):
        # Verify that the KfpPipelineProcessor supports the given export format
        self._verify_export_format(pipeline_export_format)

        t0_all = time.time()
        timestamp = datetime.now().strftime("%m%d%H%M%S")
        pipeline_name = pipeline.name
        # Create an instance id that will be used to store
        # the pipelines' dependencies, if applicable
        pipeline_instance_id = f"{pipeline_name}-{timestamp}"

        # Since pipeline_export_path may be relative to the notebook directory, ensure
        # we're using its absolute form.
        absolute_pipeline_export_path = get_absolute_path(self.root_dir, pipeline_export_path)
        runtime_configuration = self._get_metadata_configuration(
            schemaspace=Runtimes.RUNTIMES_SCHEMASPACE_ID,
            name=pipeline.runtime_config,
        )

        engine = runtime_configuration.metadata.get("engine")
        if engine == "Tekton" and not TektonClient:
            raise ValueError("kfp-tekton not installed. Please install using elyra[kfp-tekton] to use Tekton engine.")

        if os.path.exists(absolute_pipeline_export_path) and not overwrite:
            raise ValueError("File " + absolute_pipeline_export_path + " already exists.")

        self.log_pipeline_info(
            pipeline_name,
            f"Exporting pipeline as a .{pipeline_export_format} file",
        )
        # Export pipeline as static configuration file (YAML formatted)
        try:
            # Exported pipeline is not associated with an experiment
            # or a version. The association is established when the
            # pipeline is imported into KFP by the user.
            def gen_function(args):
                def new_function(*args, **kwargs):
                    self._cc_pipeline(
                        pipeline,
                        pipeline_name,
                        pipeline_instance_id=pipeline_instance_id,
                        args=args,
                    )

                params = [
                    inspect.Parameter(
                        param,
                        inspect.Parameter.POSITIONAL_OR_KEYWORD,
                        annotation=type(value),
                        default=value,
                    )
                    for param, value in args.items()
                ]
                new_function.__signature__ = inspect.Signature(params)
                new_function.__annotations__ = args
                name = Path(str(pipeline_name)).stem
                new_function.__name__ = name
                return new_function

            input_parameters = self._get_pipeline_input_parameters(pipeline)

            pipeline_function = gen_function(input_parameters)
            if engine == "Tekton":
                self.log.info("Compiling pipeline for Tekton engine")
                kfp_tekton_compiler.TektonCompiler().compile(pipeline_function, absolute_pipeline_export_path)
            else:
                self.log.info("Compiling pipeline for Argo engine")
                kfp_argo_compiler.Compiler().compile(pipeline_function, absolute_pipeline_export_path)
        except RuntimeError:
            raise
        except Exception as ex:
            if ex.__cause__:
                raise RuntimeError(str(ex)) from ex
            raise RuntimeError(
                f"Error pre-processing pipeline '{pipeline_name}' for export to '{absolute_pipeline_export_path}'",
                str(ex),
            ) from ex

        self.log_pipeline_info(
            pipeline_name,
            f"pipeline exported to '{pipeline_export_path}'",
            duration=(time.time() - t0_all),
        )

        return pipeline_export_path  # Return the input value, not its absolute form

    def _collect_envs(self, operation: Operation, **kwargs) -> Dict:
        """
        Amends envs collected from superclass with those pertaining to this subclass

        :return: dictionary containing environment name/value pairs
        """
        envs = super()._collect_envs(operation, **kwargs)
        # Only Unix-style path spec is supported.
        envs["ELYRA_WRITABLE_CONTAINER_DIR"] = self.WCD
        return envs

    def _is_next(self, key: str, links: list):
        for link in links:
            if link == key:
                return True
        return False

    def _get_next(self, parent_dict: dict, pipeline_operations: dict):
        for key in parent_dict:
            for pipeline_operation_id in pipeline_operations:
                if self._is_next(
                    key,
                    pipeline_operations[pipeline_operation_id].parent_operation_ids,
                ):
                    parent_dict[key][pipeline_operation_id] = {}
                    self._get_next(parent_dict[key], pipeline_operations)

    def _gen_link_ref_dict(self, pipeline_operations: dict):
        link_dict = {}
        operations_cache = pipeline_operations
        for operation_id in operations_cache:
            if operations_cache[operation_id].parent_operation_ids:
                continue
            link_dict[operation_id] = {}
        self._get_next(link_dict, pipeline_operations)
        return link_dict

    def _filter_special_node(
        self,
        pipeline_operations: dict,
        link_dict: dict,
        outermost_node_ids: list,
        special_node_links: dict,
        special_node_sublinks: dict,
        special_node_subnodes: dict,
        subnodes: list,
        is_sorted: list,
    ):
        global loop_stack
        for node_id in link_dict:
            if pipeline_operations[node_id].classifier.startswith("branch"):
                special_node_subnodes[node_id] = []
                special_node_sublinks[node_id] = {}
                self._filter_special_node(
                    pipeline_operations,
                    link_dict[node_id],
                    outermost_node_ids,
                    special_node_links,
                    special_node_sublinks[node_id],
                    special_node_subnodes,
                    special_node_subnodes[node_id],
                    is_sorted,
                )
            elif pipeline_operations[node_id].classifier.startswith("loop_start"):
                special_node_subnodes[node_id] = []
                special_node_sublinks[node_id] = {}
                loop_stack.append(
                    [
                        node_id,
                        special_node_sublinks[node_id],
                        special_node_subnodes[node_id],
                    ]
                )
                self._filter_special_node(
                    pipeline_operations,
                    link_dict[node_id],
                    outermost_node_ids,
                    special_node_links,
                    special_node_sublinks[node_id],
                    special_node_subnodes,
                    special_node_subnodes[node_id],
                    is_sorted,
                )
            elif pipeline_operations[node_id].classifier.startswith("loop_end"):
                if node_id not in is_sorted:
                    for id in link_dict[node_id]:
                        pipeline_operations[id].parent_operation_ids.append(loop_stack[-1][0])
                    if len(loop_stack) == 1:
                        loop_stack = []
                        self._filter_component_node(
                            pipeline_operations,
                            link_dict[node_id],
                            outermost_node_ids,
                            special_node_links,
                            special_node_subnodes,
                            is_sorted,
                        )
                    else:
                        loop_stack.pop()
                        self._filter_special_node(
                            pipeline_operations,
                            link_dict[node_id],
                            outermost_node_ids,
                            special_node_links,
                            loop_stack[-1][1],
                            special_node_subnodes,
                            loop_stack[-1][2],
                            is_sorted,
                        )
            else:
                self._filter_special_node(
                    pipeline_operations,
                    link_dict[node_id],
                    outermost_node_ids,
                    special_node_links,
                    special_node_sublinks,
                    special_node_subnodes,
                    subnodes,
                    is_sorted,
                )

            is_sorted.append(node_id)
            if node_id not in subnodes:
                subnodes.append(node_id)

    def _filter_component_node(
        self,
        pipeline_operations: dict,
        link_dict: dict,
        outermost_node_ids: list,
        special_node_links: dict,
        special_node_subnodes: dict,
        is_sorted: list,
    ):
        global loop_stack
        for node_id in link_dict:
            if node_id not in is_sorted:
                if pipeline_operations[node_id].classifier.startswith("branch"):
                    special_node_subnodes[node_id] = []
                    special_node_links[node_id] = {}
                    outermost_node_ids.append(node_id)
                    self._filter_special_node(
                        pipeline_operations,
                        link_dict[node_id],
                        outermost_node_ids,
                        special_node_links,
                        special_node_links[node_id],
                        special_node_subnodes,
                        special_node_subnodes[node_id],
                        is_sorted,
                    )
                elif pipeline_operations[node_id].classifier.startswith("loop_start"):
                    special_node_subnodes[node_id] = []
                    special_node_links[node_id] = {}
                    outermost_node_ids.append(node_id)
                    loop_stack.append(
                        [
                            node_id,
                            special_node_links[node_id],
                            special_node_subnodes[node_id],
                        ]
                    )
                    self._filter_special_node(
                        pipeline_operations,
                        link_dict[node_id],
                        outermost_node_ids,
                        special_node_links,
                        special_node_links[node_id],
                        special_node_subnodes,
                        special_node_subnodes[node_id],
                        is_sorted,
                    )
                else:
                    outermost_node_ids.append(node_id)
                    self._filter_component_node(
                        pipeline_operations,
                        link_dict[node_id],
                        outermost_node_ids,
                        special_node_links,
                        special_node_subnodes,
                        is_sorted,
                    )
                is_sorted.append(node_id)
            else:
                continue

    def _get_pipeline_input_parameters(self, pipeline):
        input_parameters = {}
        if "input_parameters" in pipeline.pipeline_parameters:
            for item in pipeline.pipeline_parameters.get("input_parameters"):
                if "value" in item.get("type"):
                    temp_value = item.get("type").get("value")
                    if item.get("type").get("widget") == "Float":
                        temp_value = float(temp_value)
                    elif item.get("type").get("widget") == "Integer":
                        temp_value = int(temp_value)
                    elif item.get("type").get("widget") == "List":
                        temp_value = self._process_list_value(temp_value)
                    input_parameters[item.get("name")] = temp_value
                else:
                    if item.get("type").get("widget") == "String":
                        input_parameters[item.get("name")] = ""
                    elif item.get("type").get("widget") == "Float":
                        input_parameters[item.get("name")] = 0.0
                    elif item.get("type").get("widget") == "List":
                        input_parameters[item.get("name")] = []
                    else:
                        input_parameters[item.get("name")] = 0
        return input_parameters

    def _filter_duplicate_nodes(
        self,
        special_node_links: dict,
        special_node_subnodes: dict,
        is_sorted: list,
    ):
        for node in special_node_links:
            self._filter_duplicate_nodes(special_node_links[node], special_node_subnodes, is_sorted)
            if node not in is_sorted:
                temp_remove_nodes = []
                for subnode in special_node_subnodes[node]:
                    if subnode not in is_sorted:
                        is_sorted.append(subnode)
                    else:
                        temp_remove_nodes.append(subnode)
                for remove_node in temp_remove_nodes:
                    special_node_subnodes[node].remove(remove_node)

    def _sorted_opreation_list(self, node_ids: list, pipeline):
        node_operations = {}
        for node_id in node_ids:
            node_operations[node_id] = pipeline.operations[node_id]
        sorted_node_operations = PipelineProcessor._sort_operations(node_operations)
        temp_operations = []
        for sorted_node_operation in sorted_node_operations:
            if sorted_node_operation.classifier.startswith("branch"):
                temp_operations.append(sorted_node_operation)
        for temp_operation in temp_operations:
            sorted_node_operations.remove(temp_operation)
        sorted_node_operations += temp_operations
        return sorted_node_operations

    def _graph_parse(
        self,
        pipeline,
    ):
        link_ref = self._gen_link_ref_dict(pipeline.operations)
        outermost_node_ids = []
        special_node_links = {}
        special_node_subnodes = {}
        is_sorted = []

        self._filter_component_node(
            pipeline.operations,
            link_ref,
            outermost_node_ids,
            special_node_links,
            special_node_subnodes,
            is_sorted,
        )

        is_sorted = []
        self._filter_duplicate_nodes(special_node_links, special_node_subnodes, is_sorted)

        temp_remove_nodes = []
        for outermost_node_id in outermost_node_ids:
            if outermost_node_id in is_sorted:
                temp_remove_nodes.append(outermost_node_id)
        for remove_node in temp_remove_nodes:
            outermost_node_ids.remove(remove_node)

        sorted_outermost_node_operations = self._sorted_opreation_list(outermost_node_ids, pipeline)
        sorted_special_node_subnodes = {}
        for node_id in special_node_subnodes:
            sorted_node_operations = self._sorted_opreation_list(special_node_subnodes[node_id], pipeline)
            sorted_special_node_subnodes[node_id] = sorted_node_operations

        return sorted_outermost_node_operations, sorted_special_node_subnodes

    def _process_component(
        self,
        args,
        operation,
        pipeline,
        container_runtime,
        emptydir_volume_size,
        cos_secret,
        cos_username,
        cos_password,
        cos_endpoint,
        cos_bucket,
        runtime_configuration,
        pipeline_name,
        experiment_name,
        artifact_object_prefix,
        pipeline_version,
        engine,
        export,
        target_ops,
    ):
        if container_runtime:
            # Volume size to create when using CRI-o, NOTE: IBM Cloud minimum is 20Gi
            emptydir_volume_size = "20Gi"

        sanitized_operation_name = self._sanitize_operation_name(operation.name)

        # Create pipeline operation
        # If operation is one of the "generic" set of NBs or scripts, construct custom ExecuteFileOp
        if isinstance(operation, GenericOperation):
            component = ComponentCache.get_generic_component_from_op(operation.classifier)

            # Collect env variables
            pipeline_envs = self._collect_envs(
                operation,
                cos_secret=cos_secret,
                cos_username=cos_username,
                cos_password=cos_password,
            )

            operation_artifact_archive = self._get_dependency_archive_name(operation)

            self.log.debug(
                f"Creating pipeline component archive '{operation_artifact_archive}' for operation '{operation}'"
            )

            container_op = ExecuteFileOp(
                name=sanitized_operation_name,
                pipeline_name=pipeline_name,
                experiment_name=experiment_name,
                notebook=operation.filename,
                cos_endpoint=cos_endpoint,
                cos_bucket=cos_bucket,
                cos_directory=artifact_object_prefix,
                cos_dependencies_archive=operation_artifact_archive,
                pipeline_version=pipeline_version,
                pipeline_source=pipeline.source,
                pipeline_inputs=operation.inputs,
                pipeline_outputs=operation.outputs,
                pipeline_envs=pipeline_envs,
                emptydir_volume_size=emptydir_volume_size,
                cpu_request=operation.cpu,
                npu310_request=operation.npu310,
                npu910_request=operation.npu910,
                mem_request=operation.memory,
                gpu_limit=operation.gpu,
                node_selector=operation.node_selector,
                workflow_engine=engine,
                image=operation.runtime_image,
                file_outputs={
                    "mlpipeline-metrics": f"{pipeline_envs['ELYRA_WRITABLE_CONTAINER_DIR']}/mlpipeline-metrics.json",  # noqa
                    "mlpipeline-ui-metadata": f"{pipeline_envs['ELYRA_WRITABLE_CONTAINER_DIR']}/mlpipeline-ui-metadata.json",  # noqa
                },
            )

            if cos_secret and not export:
                container_op.apply(use_aws_secret(cos_secret))

            image_namespace = self._get_metadata_configuration(RuntimeImages.RUNTIME_IMAGES_SCHEMASPACE_ID)
            for image_instance in image_namespace:
                if image_instance.metadata.get("image_name") == operation.runtime_image and image_instance.metadata.get(
                    "pull_policy"
                ):
                    container_op.container.set_image_pull_policy(image_instance.metadata.get("pull_policy"))

            self.log_pipeline_info(
                pipeline_name,
                f"processing operation dependencies for id '{operation.id}'",
                operation_name=operation.name,
            )
            self._upload_dependencies_to_object_store(
                runtime_configuration, pipeline_name, operation, prefix=artifact_object_prefix
            )

        # If operation is a "non-standard" component, load it's spec and create operation with factory function
        else:
            global global_loop_args
            # Retrieve component from cache
            component = ComponentCache.instance().get_component(self._type, operation.classifier)
            # Convert the user-entered value of certain properties according to their type
            for component_property in component.properties:
                self.log.debug(
                    f"Processing component parameter '{component_property.name}' "
                    f"of type '{component_property.json_data_type}'"
                )

                if component_property.allowed_input_types == [None]:
                    # Outputs are skipped
                    continue

                # Get corresponding property's value from parsed pipeline
                property_value_dict = operation.component_params.get(component_property.ref)
                data_entry_type = property_value_dict.get("widget", None)  # one of: inputpath, file, raw data type
                property_value = property_value_dict.get("value", None)
                if data_entry_type == "inputpath":
                    # KFP path-based parameters accept an input from a parent
                    output_node_id = property_value.get("value")  # parent node id
                    output_node_parameter_key = property_value.get("option").replace("output_", "")  # parent param
                    if target_ops[output_node_id] == "Loop Start":
                        arg = property_value.get("option").replace("ParallelFor:", "")
                        if arg == "item":
                            operation.component_params[component_property.ref] = global_loop_args[output_node_id]
                        else:
                            arg = arg.replace("item.", "")
                            operation.component_params[component_property.ref] = getattr(
                                global_loop_args[output_node_id], arg
                            )
                    else:
                        operation.component_params[component_property.ref] = target_ops[output_node_id].outputs[
                            output_node_parameter_key
                        ]
                elif data_entry_type == "enum":
                    for arg in args:
                        if arg.name == property_value:
                            operation.component_params[component_property.ref] = arg
                else:  # Parameter is either of a raw data type or file contents
                    if data_entry_type == "file" and property_value:
                        # Read a value from a file
                        absolute_path = get_absolute_path(self.root_dir, property_value)
                        with open(absolute_path, "r") as f:
                            property_value = f.read() if os.path.getsize(absolute_path) else None

                    # If the value is not found, assign it the default value assigned in parser
                    if property_value is None:
                        property_value = component_property.value

                    # Process the value according to its type, if necessary
                    if component_property.json_data_type == "object":
                        processed_value = self._process_dictionary_value(property_value)
                        operation.component_params[component_property.ref] = processed_value
                    elif component_property.json_data_type == "array":
                        processed_value = self._process_list_value(property_value)
                        operation.component_params[component_property.ref] = processed_value
                    else:
                        operation.component_params[component_property.ref] = property_value

            # Build component task factory
            try:
                factory_function = components.load_component_from_text(component.definition)
            except Exception as e:
                # TODO Fix error messaging and break exceptions down into categories
                self.log.error(f"Error loading component spec for {operation.name}: {str(e)}")
                raise RuntimeError(f"Error loading component spec for {operation.name}.")

            # Add factory function, which returns a ContainerOp task instance, to pipeline operation dict
            try:
                comp_spec_inputs = [
                    inputs.name.lower().replace(" ", "_") for inputs in factory_function.component_spec.inputs or []
                ]

                # Remove inputs and outputs from params dict
                # TODO: need to have way to retrieve only required params
                parameter_removal_list = ["inputs", "outputs"]
                resources = {}
                for key, value in operation.component_params.items():
                    resources[key] = value

                for component_param in operation.component_params_as_dict.keys():
                    if component_param not in comp_spec_inputs:
                        parameter_removal_list.append(component_param)

                for parameter in parameter_removal_list:
                    operation.component_params_as_dict.pop(parameter, None)

                # Create ContainerOp instance and assign appropriate user-provided name
                sanitized_component_params = {
                    self._sanitize_param_name(name): value for name, value in operation.component_params_as_dict.items()
                }
                container_op = factory_function(**sanitized_component_params)
                container_op.set_display_name(operation.name)

                if "cpu" in resources:
                    container_op.set_cpu_request(cpu=str(resources.get("cpu")))
                if "npu310" in resources:
                    container_op.add_resource_request("huawei.com/Ascend310", str(resources.get("npu310")))
                    container_op.add_resource_limit("huawei.com/Ascend310", str(resources.get("npu310")))
                if "npu910" in resources:
                    container_op.add_resource_request("huawei.com/Ascend910", str(resources.get("npu910")))
                    container_op.add_resource_limit("huawei.com/Ascend910", str(resources.get("npu910")))
                if "memory" in resources:
                    container_op.set_memory_request(memory=str(resources.get("memory")) + "G")
                if "gpu" in resources:
                    gpu_vendor = "nvidia"
                    container_op.set_gpu_limit(gpu=str(resources.get("gpu")), vendor=gpu_vendor)
                if "node_selector" in resources:
                    for key, value in resources.get("node_selector").items():
                        container_op.add_node_selector_constraint(key, value)

            except Exception as e:
                # TODO Fix error messaging and break exceptions down into categories
                self.log.error(f"Error constructing component {operation.name}: {str(e)}")
                raise RuntimeError(f"Error constructing component {operation.name}.")

        # Attach node comment
        if operation.doc:
            container_op.add_pod_annotation("elyra/node-user-doc", operation.doc)

        # Process Elyra-owned properties as required for each type
        for value in operation.elyra_params.values():
            if isinstance(value, (ElyraProperty, ElyraPropertyList)):
                value.add_to_execution_object(
                    runtime_processor=self,
                    execution_object=container_op,
                    pipeline_input_parameters=args,
                )

        # Add ContainerOp to target_ops dict
        target_ops[operation.id] = container_op

        for parent_operation_id in operation.parent_operation_ids:
            if parent_operation_id in target_ops:
                parent_op = target_ops[parent_operation_id]
                if not isinstance(parent_op, str):
                    container_op.after(parent_op)

    def _loop(
        self,
        args,
        sorted_node_operations,
        sorted_special_node_subnodes,
        pipeline,
        container_runtime,
        emptydir_volume_size,
        cos_secret,
        cos_username,
        cos_password,
        cos_endpoint,
        cos_bucket,
        runtime_configuration,
        pipeline_name,
        experiment_name,
        artifact_object_prefix,
        pipeline_version,
        engine,
        export,
        target_ops,
    ):
        for operation in sorted_node_operations:
            if operation.classifier.startswith("branch"):
                self._process_branch(
                    args,
                    operation,
                    sorted_special_node_subnodes[operation.id],
                    sorted_special_node_subnodes,
                    pipeline,
                    container_runtime,
                    emptydir_volume_size,
                    cos_secret,
                    cos_username,
                    cos_password,
                    cos_endpoint,
                    cos_bucket,
                    runtime_configuration,
                    pipeline_name,
                    experiment_name,
                    artifact_object_prefix,
                    pipeline_version,
                    engine,
                    export,
                    target_ops,
                )
            elif operation.classifier.startswith("loop_start"):
                self._process_loop(
                    args,
                    operation,
                    sorted_special_node_subnodes[operation.id],
                    sorted_special_node_subnodes,
                    pipeline,
                    container_runtime,
                    emptydir_volume_size,
                    cos_secret,
                    cos_username,
                    cos_password,
                    cos_endpoint,
                    cos_bucket,
                    runtime_configuration,
                    pipeline_name,
                    experiment_name,
                    artifact_object_prefix,
                    pipeline_version,
                    engine,
                    export,
                    target_ops,
                )
            elif operation.classifier.startswith("loop_end"):
                pass
            else:
                self._process_component(
                    args,
                    operation,
                    pipeline,
                    container_runtime,
                    emptydir_volume_size,
                    cos_secret,
                    cos_username,
                    cos_password,
                    cos_endpoint,
                    cos_bucket,
                    runtime_configuration,
                    pipeline_name,
                    experiment_name,
                    artifact_object_prefix,
                    pipeline_version,
                    engine,
                    export,
                    target_ops,
                )

    def _parse_branch_parameter(self, parameter, args, target_ops):
        global global_loop_args
        if parameter.get("widget") == "string":
            return parameter.get("value")
        elif parameter.get("widget") == "enum":
            for arg in args:
                if arg.name == parameter.get("value"):
                    return arg
        elif parameter.get("widget") == "inputpath":
            output_node_id = parameter.get("value").get("value")
            output_node_parameter_key = parameter.get("value").get("option").replace("output_", "")
            if target_ops[output_node_id] == "Loop Start":
                arg = parameter.get("value").get("option").replace("ParallelFor:", "")
                if arg == "item":
                    return global_loop_args[output_node_id]
                else:
                    arg = arg.replace("item.", "")
                    return getattr(global_loop_args[output_node_id], arg)
            else:
                return target_ops[output_node_id].outputs[output_node_parameter_key]

    def _parse_loop_parameter(self, parameter, args, target_ops):
        if parameter.get("widget") == "Number":
            return [i for i in range(int(parameter.get("value")))]
        elif parameter.get("widget") == "List[str|int|float]":
            return self._process_list_value(parameter.get("value"))
        elif parameter.get("widget") == "List[Dict[str, any]]":
            loop_args = []
            loop_arg = {}
            for items in parameter.get("value"):
                for item in items:
                    loop_arg[item.get("key")] = item.get("value")
                loop_args.append(loop_arg)
            return loop_args
        elif parameter.get("widget") == "enum":
            for arg in args:
                if arg.name == parameter.get("value"):
                    return arg
        elif parameter.get("widget") == "inputpath":
            output_node_id = parameter.get("value").get("value")
            output_node_parameter_key = parameter.get("value").get("option").replace("output_", "")
            return target_ops[output_node_id].outputs[output_node_parameter_key]

    @staticmethod
    def get_operator_fn(op):
        return {
            "==": operator.eq,
            "!=": operator.ne,
            ">": operator.gt,
            ">=": operator.ge,
            "<": operator.lt,
            "<=": operator.le,
        }[op]

    def _process_branch(
        self,
        args,
        operation,
        sorted_node_operations,
        sorted_special_node_subnodes,
        pipeline,
        container_runtime,
        emptydir_volume_size,
        cos_secret,
        cos_username,
        cos_password,
        cos_endpoint,
        cos_bucket,
        runtime_configuration,
        pipeline_name,
        experiment_name,
        artifact_object_prefix,
        pipeline_version,
        engine,
        export,
        target_ops,
    ):
        name = operation.name
        if name == "Pipeline Branch":
            name = None
        target_ops[operation.id] = "branch"
        component_params = operation.component_params
        branch_parameter1 = self._parse_branch_parameter(
            component_params.get("branch_conditions").get("branch_parameter1"),
            args,
            target_ops,
        )
        branch_parameter2 = self._parse_branch_parameter(
            component_params.get("branch_conditions").get("branch_parameter2"),
            args,
            target_ops,
        )
        operate = component_params.get("branch_conditions").get("operate")

        with dsl.Condition(
            self.get_operator_fn(operate)(branch_parameter1, branch_parameter2),
            name,
        ):
            self._loop(
                args,
                sorted_node_operations,
                sorted_special_node_subnodes,
                pipeline,
                container_runtime,
                emptydir_volume_size,
                cos_secret,
                cos_username,
                cos_password,
                cos_endpoint,
                cos_bucket,
                runtime_configuration,
                pipeline_name,
                experiment_name,
                artifact_object_prefix,
                pipeline_version,
                engine,
                export,
                target_ops,
            )

    def _process_loop(
        self,
        args,
        operation,
        sorted_node_operations,
        sorted_special_node_subnodes,
        pipeline,
        container_runtime,
        emptydir_volume_size,
        cos_secret,
        cos_username,
        cos_password,
        cos_endpoint,
        cos_bucket,
        runtime_configuration,
        pipeline_name,
        experiment_name,
        artifact_object_prefix,
        pipeline_version,
        engine,
        export,
        target_ops,
    ):
        global global_loop_args
        target_ops[operation.id] = "Loop Start"
        component_params = operation.component_params
        loop_args = self._parse_loop_parameter(component_params.get("loop_args"), args, target_ops)
        parallelism = None
        if "parallelism" in component_params:
            parallelism = int(component_params.get("parallelism"))

        with dsl.ParallelFor(loop_args, parallelism) as item:
            global_loop_args[operation.id] = item
            self._loop(
                args,
                sorted_node_operations,
                sorted_special_node_subnodes,
                pipeline,
                container_runtime,
                emptydir_volume_size,
                cos_secret,
                cos_username,
                cos_password,
                cos_endpoint,
                cos_bucket,
                runtime_configuration,
                pipeline_name,
                experiment_name,
                artifact_object_prefix,
                pipeline_version,
                engine,
                export,
                target_ops,
            )

    def _cc_pipeline(
        self,
        pipeline: Pipeline,
        pipeline_name: str,
        pipeline_version: str = "",
        experiment_name: str = "",
        pipeline_instance_id: str = None,
        export=False,
        args=None,
    ):
        runtime_configuration = self._get_metadata_configuration(
            schemaspace=Runtimes.RUNTIMES_SCHEMASPACE_ID,
            name=pipeline.runtime_config,
        )

        cos_endpoint = runtime_configuration.metadata.get("cos_endpoint")
        cos_username = runtime_configuration.metadata.get("cos_username")
        cos_password = runtime_configuration.metadata.get("cos_password")
        cos_secret = runtime_configuration.metadata.get("cos_secret")
        cos_bucket = runtime_configuration.metadata.get("cos_bucket")
        engine = runtime_configuration.metadata.get("engine")

        pipeline_instance_id = pipeline_instance_id or pipeline_name

        artifact_object_prefix = join_paths(
            pipeline.pipeline_parameters.get(pipeline_constants.COS_OBJECT_PREFIX),
            pipeline_instance_id,
        )

        self.log_pipeline_info(
            pipeline_name,
            f"processing pipeline dependencies for upload to '{cos_endpoint}' "
            f"bucket '{cos_bucket}' folder '{artifact_object_prefix}'",
        )
        t0_all = time.time()

        emptydir_volume_size = ""
        container_runtime = bool(os.getenv("CRIO_RUNTIME", "False").lower() == "true")

        # Create dictionary that maps component Id to its ContainerOp instance
        target_ops = {}

        # Sort operations based on dependency graph (topological order)
        sorted_operations = PipelineProcessor._sort_operations(pipeline.operations)

        # Determine whether access to cloud storage is required
        for operation in sorted_operations:
            if isinstance(operation, GenericOperation):
                self._verify_cos_connectivity(runtime_configuration)
                break

        sorted_outermost_node_operations, sorted_special_node_subnodes = self._graph_parse(pipeline)

        # All previous operation outputs should be propagated throughout the pipeline.
        # In order to process this recursively, the current operation's inputs should be combined
        # from its parent's inputs (which, themselves are derived from the outputs of their parent)
        # and its parent's outputs.

        PipelineProcessor._propagate_operation_inputs_outputs(pipeline, sorted_operations)

        self._loop(
            args,
            sorted_outermost_node_operations,
            sorted_special_node_subnodes,
            pipeline,
            container_runtime,
            emptydir_volume_size,
            cos_secret,
            cos_username,
            cos_password,
            cos_endpoint,
            cos_bucket,
            runtime_configuration,
            pipeline_name,
            experiment_name,
            artifact_object_prefix,
            pipeline_version,
            engine,
            export,
            target_ops,
        )

        self.log_pipeline_info(
            pipeline_name,
            "pipeline dependencies processed",
            duration=(time.time() - t0_all),
        )

        return target_ops

    def _generate_pipeline_conf(self, pipeline: dict) -> PipelineConf:
        """
        Returns a KFP pipeline configuration for this pipeline, which can be empty.

        :param pipeline: pipeline dictionary
        :type pipeline: dict
        :return: https://kubeflow-pipelines.readthedocs.io/en/latest/source/kfp.dsl.html#kfp.dsl.PipelineConf
        :rtype: kfp.dsl import PipelineConf
        """

        self.log.debug("Generating pipeline configuration ...")
        pipeline_conf = PipelineConf()

        #
        # Gather input for container image pull secrets in support of private container image registries
        # https://kubeflow-pipelines.readthedocs.io/en/latest/source/kfp.dsl.html#kfp.dsl.PipelineConf.set_image_pull_secrets
        #
        image_namespace = self._get_metadata_configuration(schemaspace=RuntimeImages.RUNTIME_IMAGES_SCHEMASPACE_ID)

        # iterate through pipeline operations and create list of Kubernetes secret names
        # that are associated with generic components
        container_image_pull_secret_names = []
        for operation in pipeline.operations.values():
            if isinstance(operation, GenericOperation):
                for image_instance in image_namespace:
                    if image_instance.metadata.get("image_name") == operation.runtime_image:
                        if image_instance.metadata.get("pull_secret"):
                            container_image_pull_secret_names.append(image_instance.metadata.get("pull_secret"))
                        break

        if len(container_image_pull_secret_names) > 0:
            # de-duplicate the pull secret name list, create Kubernetes resource
            # references and add them to the pipeline configuration
            container_image_pull_secrets = []
            for secret_name in list(set(container_image_pull_secret_names)):
                container_image_pull_secrets.append(k8s_client.V1ObjectReference(name=secret_name))
            pipeline_conf.set_image_pull_secrets(container_image_pull_secrets)
            self.log.debug(
                f"Added {len(container_image_pull_secrets)}" " image pull secret(s) to the pipeline configuration."
            )

        return pipeline_conf

    @staticmethod
    def _sanitize_operation_name(name: str) -> str:
        """
        In KFP, only letters, numbers, spaces, "_", and "-" are allowed in name.
        :param name: name of the operation
        """
        return re.sub("-+", "-", re.sub("[^-_0-9A-Za-z ]+", "-", name)).lstrip("-").rstrip("-")

    @staticmethod
    def _sanitize_param_name(name: str) -> str:
        """
        Sanitize a component parameter name.

        Behavior is mirrored from how Kubeflow 1.X sanitizes identifier names:
        - https://github.com/kubeflow/pipelines/blob/1.8.1/sdk/python/kfp/components/_naming.py#L32-L42
        - https://github.com/kubeflow/pipelines/blob/1.8.1/sdk/python/kfp/components/_naming.py#L49-L50
        """
        normalized_name = name.lower()

        # remove non-word characters
        normalized_name = re.sub(r"[\W_]", " ", normalized_name)

        # no double spaces, leading or trailing spaces
        normalized_name = re.sub(" +", " ", normalized_name).strip()

        # no leading digits
        if re.match(r"\d", normalized_name):
            normalized_name = "n" + normalized_name

        return normalized_name.replace(" ", "_")

    def add_disable_node_caching(self, instance: DisableNodeCaching, execution_object: Any, **kwargs) -> None:
        """Add DisableNodeCaching info to the execution object for the given runtime processor"""
        # Force re-execution of the operation by setting staleness to zero days
        # https://www.kubeflow.org/docs/components/pipelines/overview/caching/#managing-caching-staleness
        if instance.selection:
            execution_object.execution_options.caching_strategy.max_cache_staleness = "P0D"

    def add_env_var(
        self,
        instance: EnvironmentVariable,
        execution_object: Any,
        pipeline_input_parameters: Any,
        **kwargs,
    ) -> None:
        """Add KubernetesLabel instance to the execution object for the given runtime processor"""

        v1_env_var = V1EnvVar(name=instance.env_var, value=instance.value)
        if instance.type == "enum" and instance.value:
            for pipeline_input_parameter in pipeline_input_parameters:
                if pipeline_input_parameter.name == instance.value:
                    v1_env_var = V1EnvVar(name=instance.env_var, value=pipeline_input_parameter)
        execution_object.container.add_env_variable(v1_env_var)

    def add_kubernetes_secret(
        self,
        instance: KubernetesSecret,
        execution_object: Any,
        pipeline_input_parameters: Any,
        **kwargs,
    ) -> None:
        """Add KubernetesSecret instance to the execution object for the given runtime processor"""
        name = instance.name
        key = instance.key
        if instance.key_type == "enum" and instance.key:
            for pipeline_input_parameter in pipeline_input_parameters:
                if pipeline_input_parameter.name == instance.key:
                    key = pipeline_input_parameter
        if instance.name_type == "enum" and instance.name:
            for pipeline_input_parameter in pipeline_input_parameters:
                if pipeline_input_parameter.name == instance.name:
                    name = pipeline_input_parameter
        v1_env_var = V1EnvVar(
            name=instance.env_var,
            value_from=V1EnvVarSource(secret_key_ref=V1SecretKeySelector(name=name, key=key)),
        )
        execution_object.container.add_env_variable(v1_env_var)

    def add_mounted_volume(
        self,
        instance: VolumeMount,
        execution_object: Any,
        pipeline_input_parameters: Any,
        **kwargs,
    ) -> None:
        """Add VolumeMount instance to the execution object for the given runtime processor"""

        volume = V1Volume(
            name=instance.pvc_name,
            persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(claim_name=instance.pvc_name),
        )
        volume_mount = V1VolumeMount(mount_path=instance.path, name=instance.pvc_name)

        if instance.type == "enum" and instance.pvc_name:
            for pipeline_input_parameter in pipeline_input_parameters:
                if pipeline_input_parameter.name == instance.pvc_name:
                    volume = V1Volume(
                        name=pipeline_input_parameter,
                        persistent_volume_claim=V1PersistentVolumeClaimVolumeSource(
                            claim_name=pipeline_input_parameter
                        ),
                    )
                    volume_mount = V1VolumeMount(mount_path=instance.path, name=pipeline_input_parameter)
        if volume not in execution_object.volumes:
            execution_object.add_volume(volume)
        execution_object.container.add_volume_mount(volume_mount)

    def add_kubernetes_pod_annotation(
        self,
        instance: KubernetesAnnotation,
        execution_object: Any,
        pipeline_input_parameters: Any,
        **kwargs,
    ) -> None:
        """Add KubernetesAnnotation instance to the execution object for the given runtime processor"""
        if instance.key not in execution_object.pod_annotations:
            if instance.type == "enum" and instance.value:
                for pipeline_input_parameter in pipeline_input_parameters:
                    if pipeline_input_parameter.name == instance.value:
                        execution_object.add_pod_annotation(instance.key, pipeline_input_parameter)
            else:
                execution_object.add_pod_annotation(instance.key, instance.value or "")

    def add_kubernetes_pod_label(
        self,
        instance: KubernetesLabel,
        execution_object: Any,
        pipeline_input_parameters: Any,
        **kwargs,
    ) -> None:
        """Add KubernetesLabel instance to the execution object for the given runtime processor"""
        if instance.key not in execution_object.pod_labels:
            if instance.type == "enum" and instance.value:
                for pipeline_input_parameter in pipeline_input_parameters:
                    if pipeline_input_parameter.name == instance.value:
                        execution_object.add_pod_label(instance.key, pipeline_input_parameter)
            else:
                execution_object.add_pod_label(instance.key, instance.value or "")

    def add_kubernetes_toleration(self, instance: KubernetesToleration, execution_object: Any, **kwargs) -> None:
        """Add KubernetesToleration instance to the execution object for the given runtime processor"""
        toleration = V1Toleration(
            effect=instance.effect,
            key=instance.key,
            operator=instance.operator,
            value=instance.value,
        )
        if toleration not in execution_object.tolerations:
            execution_object.add_toleration(toleration)

    @property
    def supported_properties(self) -> Set[str]:
        """A list of Elyra-owned properties supported by this runtime processor."""
        return [
            pipeline_constants.DISABLE_NODE_CACHING,
            pipeline_constants.ENV_VARIABLES,
            pipeline_constants.MOUNTED_VOLUMES,
            pipeline_constants.KUBERNETES_SECRETS,
            pipeline_constants.KUBERNETES_POD_ANNOTATIONS,
            pipeline_constants.KUBERNETES_POD_LABELS,
            pipeline_constants.KUBERNETES_TOLERATIONS,
        ]


class KfpPipelineProcessorResponse(RuntimePipelineProcessorResponse):
    _type = RuntimeProcessorType.KUBEFLOW_PIPELINES
    _name = "kfp"

    def __init__(self, run_id, run_url, object_storage_url, object_storage_path):
        super().__init__(run_url, object_storage_url, object_storage_path)
        self.run_id = run_id

    def to_json(self):
        response = super().to_json()
        response["run_id"] = self.run_id
        return response
