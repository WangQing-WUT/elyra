from elyra.pipeline.processor import PipelineProcessor
from elyra.pipeline.processor import PipelineProcessorResponse
from elyra.pipeline.runtime_type import RuntimeProcessorType


class WfpPipelineProcessor(PipelineProcessor):

    _type = RuntimeProcessorType.WORKFLOW_PIPELINES
    _name = "wfp"


class WfpPipelineProcessorResponse(PipelineProcessorResponse):

    _type = RuntimeProcessorType.WORKFLOW_PIPELINES
    _name = "wfp"
