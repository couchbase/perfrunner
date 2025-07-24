from perfrunner.helpers.local import run_aibench
from perfrunner.settings import AIBenchSettings, AIGatewayTargetSettings


def run_aibench_task(aibench_settings: AIBenchSettings, target: AIGatewayTargetSettings, *args):
    options = [
        f"--endpoint {aibench_settings.endpoint}",
        f"--model {aibench_settings.model_name}",
        f"--dataset {aibench_settings.dataset}",
        f"--subset {aibench_settings.subset}",
        f"--split {aibench_settings.split}" if aibench_settings.split else "",
        f"--num-requests {aibench_settings.ops}" if aibench_settings.ops else "",
        f"--concurrency {aibench_settings.workers}" if aibench_settings.workers else "",
        f"--duration {aibench_settings.time}" if aibench_settings.time else "",
        f"--max-tokens {aibench_settings.max_tokens}",
        f"--best-of {aibench_settings.best_of}",
        f"--logprobs {aibench_settings.logprobs}" if aibench_settings.logprobs else "",
        "--ignore-eos" if aibench_settings.ignore_eos else "",
        f"--encoding-format {aibench_settings.encoding_format}",
        f"--handler {aibench_settings.handler}",
        f"--tag {aibench_settings.tag}",
    ]
    run_aibench(" ".join(options), api_key=target.api_key, gateway_endpoint=target.gateway_endpoint)
