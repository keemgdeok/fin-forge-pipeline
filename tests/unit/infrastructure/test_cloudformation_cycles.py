"""Ensure synthesized CloudFormation templates have no circular dependencies.

The tests instantiate key stacks and analyse the generated CloudFormation
template dependency graph (based on ``DependsOn``, ``Ref``, and ``Fn::GetAtt``).
If a cycle is detected, the test fails with the offending path so we can catch
regressions early.
"""

from __future__ import annotations

from typing import Dict, Iterator, List, Set

import pytest
from aws_cdk import App, Stack
from aws_cdk.assertions import Template

from infrastructure.config.environments.dev import dev_config
from infrastructure.core.security_stack import SecurityStack
from infrastructure.core.shared_storage_stack import SharedStorageStack
from infrastructure.pipelines.daily_prices_data.ingestion_stack import (
    DailyPricesDataIngestionStack,
)
from infrastructure.pipelines.daily_prices_data.processing_stack import (
    DailyPricesDataProcessingStack,
)


def _collect_refs(value: object) -> Iterator[str]:
    """Yield logical IDs referenced via Ref/GetAtt inside a CFN structure."""

    if isinstance(value, dict):
        if set(value.keys()) == {"Ref"}:
            target = value["Ref"]
            if isinstance(target, str):
                yield target
        elif "Fn::GetAtt" in value:
            target = value["Fn::GetAtt"]
            if isinstance(target, list) and target:
                logical_id = target[0]
            elif isinstance(target, str):
                logical_id = target.split(".", 1)[0]
            else:
                logical_id = None
            if isinstance(logical_id, str):
                yield logical_id
        for nested in value.values():
            yield from _collect_refs(nested)
    elif isinstance(value, list):
        for item in value:
            yield from _collect_refs(item)


def _build_dependency_graph(template_dict: Dict[str, object]) -> Dict[str, Set[str]]:
    resources = template_dict.get("Resources", {})
    graph: Dict[str, Set[str]] = {name: set() for name in resources}

    for name, definition in resources.items():
        if not isinstance(definition, dict):
            continue

        deps: Set[str] = set()

        depends_on = definition.get("DependsOn")
        if isinstance(depends_on, str):
            deps.add(depends_on)
        elif isinstance(depends_on, list):
            deps.update(dep for dep in depends_on if isinstance(dep, str))

        for key in ("Properties", "Metadata"):
            section = definition.get(key)
            if section is not None:
                deps.update(_collect_refs(section))

        graph[name] = {dep for dep in deps if dep in resources and dep != name}

    return graph


def _detect_cycles(graph: Dict[str, Set[str]]) -> List[List[str]]:
    visited: Set[str] = set()
    active: Set[str] = set()
    path: List[str] = []
    cycles: List[List[str]] = []

    def dfs(node: str) -> None:
        if node in active:
            cycle_start = path.index(node)
            cycles.append(path[cycle_start:] + [node])
            return
        if node in visited:
            return

        visited.add(node)
        active.add(node)
        path.append(node)

        for neighbour in graph.get(node, ()):  # iterate dependencies
            dfs(neighbour)

        path.pop()
        active.remove(node)

    for resource in graph:
        if resource not in visited:
            dfs(resource)

    return cycles


def _assert_no_cycles(stack: Stack) -> None:
    template = Template.from_stack(stack).to_json()
    graph = _build_dependency_graph(template)
    cycles = _detect_cycles(graph)
    if cycles:
        readable = ", ".join(" -> ".join(cycle) for cycle in cycles)
        pytest.fail(f"Detected CloudFormation dependency cycle(s): {readable}")


def _create_app_with_shared_storage() -> tuple[App, SharedStorageStack]:
    app = App()
    shared_storage = SharedStorageStack(app, "SharedStorage", environment="dev", config=dev_config)
    return app, shared_storage


def test_shared_storage_stack_has_no_cycles() -> None:
    app, shared = _create_app_with_shared_storage()
    _assert_no_cycles(shared)


def test_security_stack_has_no_cycles() -> None:
    app, shared = _create_app_with_shared_storage()
    security = SecurityStack(
        app,
        "SecurityStack",
        environment="dev",
        config=dev_config,
        shared_storage_stack=shared,
    )
    _assert_no_cycles(security)


def test_ingestion_stack_has_no_cycles() -> None:
    app, shared = _create_app_with_shared_storage()
    ingestion = DailyPricesDataIngestionStack(
        app,
        "IngestionStack",
        environment="dev",
        config=dev_config,
        shared_storage_stack=shared,
        lambda_execution_role_arn="arn:aws:iam::111122223333:role/lambda",
    )
    _assert_no_cycles(ingestion)


def test_processing_stack_has_no_cycles() -> None:
    app, shared = _create_app_with_shared_storage()
    processing = DailyPricesDataProcessingStack(
        app,
        "ProcessingStack",
        environment="dev",
        config=dev_config,
        shared_storage_stack=shared,
        lambda_execution_role_arn="arn:aws:iam::111122223333:role/lambda",
        glue_execution_role_arn="arn:aws:iam::111122223333:role/glue",
        step_functions_execution_role_arn="arn:aws:iam::111122223333:role/sfn",
        batch_tracker_table=shared.batch_tracker_table,
    )
    _assert_no_cycles(processing)
