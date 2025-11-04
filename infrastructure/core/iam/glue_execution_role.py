"""Construct providing the shared Glue execution role."""

from __future__ import annotations

from aws_cdk import Stack, aws_iam as iam
from constructs import Construct

from infrastructure.config.types import EnvironmentConfig
from infrastructure.core.iam import utils as iam_utils


class GlueExecutionRoleConstruct(Construct):
    """Provision the Glue execution role with scoped S3 access."""

    def __init__(self, scope: Construct, construct_id: str, *, env_name: str, config: EnvironmentConfig) -> None:
        super().__init__(scope, construct_id)
        stack = Stack.of(self)
        account = stack.account

        raw_bucket_name = f"data-pipeline-raw-{env_name}-{account}"
        curated_bucket_name = f"data-pipeline-curated-{env_name}-{account}"
        artifacts_bucket_name = f"{env_name}-data-platform-artifacts-{account}"

        s3_policy = self._build_glue_s3_policy(
            stack=stack,
            config=config,
            raw_bucket_name=raw_bucket_name,
            curated_bucket_name=curated_bucket_name,
            artifacts_bucket_name=artifacts_bucket_name,
        )

        self._role = iam.Role(
            self,
            "Role",
            role_name=f"{env_name}-data-platform-glue-role",
            assumed_by=iam.ServicePrincipal("glue.amazonaws.com"),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AWSGlueServiceRole"),
            ],
            inline_policies={"S3DataAccess": s3_policy},
        )

    @property
    def role(self) -> iam.Role:
        """Return the created IAM role."""
        return self._role

    def _build_glue_s3_policy(
        self,
        *,
        stack: Stack,
        config: EnvironmentConfig,
        raw_bucket_name: str,
        curated_bucket_name: str,
        artifacts_bucket_name: str,
    ) -> iam.PolicyDocument:
        """Construct the Glue S3 access policy with least-privilege defaults."""
        asset_bucket_name = iam_utils.bootstrap_asset_bucket_name(stack)
        domain_tables = iam_utils.processing_domain_tables(config)
        curated_subdirs = iam_utils.curated_subdirectories(config)
        additional_patterns = iam_utils.config_string_list(config, "glue_additional_s3_patterns", default=())

        raw_read_arns: list[str] = []
        curated_read_arns: list[str] = []
        curated_write_arns: list[str] = []
        artifacts_schema_arns: list[str] = []

        if domain_tables:
            for domain, table in domain_tables:
                base_prefix = "/".join([domain, table])
                raw_read_arns.append(iam_utils.bucket_objects_arn(raw_bucket_name, base_prefix))
                curated_read_arns.append(iam_utils.bucket_objects_arn(curated_bucket_name, base_prefix))
                curated_write_arns.append(iam_utils.bucket_objects_arn(curated_bucket_name, base_prefix))
                artifacts_schema_arns.append(
                    iam_utils.bucket_objects_arn(artifacts_bucket_name, f"{domain}/{table}/_schema")
                )

                effective_subdirs = curated_subdirs or [""]
                for subdir in effective_subdirs:
                    prefix_parts = [domain, table]
                    if subdir:
                        prefix_parts.append(subdir)
                    prefix = "/".join(prefix_parts)
                    curated_write_arns.append(iam_utils.bucket_objects_arn(curated_bucket_name, prefix))
        else:
            raw_read_arns.append(iam_utils.bucket_objects_arn(raw_bucket_name))
            curated_read_arns.append(iam_utils.bucket_objects_arn(curated_bucket_name))
            curated_write_arns.append(iam_utils.bucket_objects_arn(curated_bucket_name))
            artifacts_schema_arns.append(iam_utils.bucket_objects_arn(artifacts_bucket_name, "_schema"))

        artifacts_temp_arn = iam_utils.bucket_objects_arn(artifacts_bucket_name, "temp")
        asset_objects_arn = iam_utils.bucket_objects_arn(asset_bucket_name)

        read_resources = iam_utils.dedupe(
            raw_read_arns
            + curated_read_arns
            + artifacts_schema_arns
            + [artifacts_temp_arn, asset_objects_arn]
            + additional_patterns
        )
        write_resources = iam_utils.dedupe(
            curated_write_arns + artifacts_schema_arns + [artifacts_temp_arn] + additional_patterns
        )

        list_bucket_resources = iam_utils.dedupe(
            [iam_utils.bucket_arn(raw_bucket_name), iam_utils.bucket_arn(curated_bucket_name)]
        )
        artifacts_bucket_arn = iam_utils.bucket_arn(artifacts_bucket_name)

        statements: list[iam.PolicyStatement] = []

        if list_bucket_resources:
            statements.append(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:ListBucket"],
                    resources=list_bucket_resources,
                )
            )

        statements.append(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                actions=["s3:ListBucket"],
                resources=[artifacts_bucket_arn],
                conditions={"StringLike": {"s3:prefix": ["temp", "temp/*", "temp/"]}},
            )
        )

        if read_resources:
            statements.append(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:GetObject", "s3:GetObjectVersion"],
                    resources=read_resources,
                )
            )

        if write_resources:
            statements.append(
                iam.PolicyStatement(
                    effect=iam.Effect.ALLOW,
                    actions=["s3:PutObject", "s3:DeleteObject"],
                    resources=write_resources,
                )
            )

        return iam.PolicyDocument(statements=statements)
