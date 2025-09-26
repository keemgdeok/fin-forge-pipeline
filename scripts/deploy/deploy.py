#!/usr/bin/env python3
"""Deployment script for the data pipeline CDK application."""

import argparse
import os
import shlex
import subprocess
import sys
from typing import Mapping, Optional, Sequence


def run_command(
    command: Sequence[str], *, check: bool = True, env: Optional[Mapping[str, str]] = None
) -> subprocess.CompletedProcess[str]:
    """Execute a subprocess without shell interpolation."""

    printable = " ".join(shlex.quote(part) for part in command)
    print(f"Running: {printable}")

    result = subprocess.run(command, capture_output=True, text=True, env=env, check=False)

    if check and result.returncode != 0:
        print(f"Command failed with return code {result.returncode}")
        if result.stdout:
            print(f"stdout: {result.stdout}")
        if result.stderr:
            print(f"stderr: {result.stderr}")
        sys.exit(result.returncode)

    return result


def deploy_stacks(environment: str, stacks: Optional[str] = None) -> None:
    """Deploy CDK stacks to the specified environment."""
    print(f"Deploying to environment: {environment}")

    exec_env = {**os.environ, "CDK_DEFAULT_REGION": "ap-northeast-2"}

    # Bootstrap CDK if needed
    print("Checking CDK bootstrap status...")
    run_command(
        ["cdk", "bootstrap", "--context", f"environment={environment}"],
        check=False,
        env=exec_env,
    )

    # Deploy stacks
    deploy_cmd = ["cdk", "deploy"]
    if stacks:
        deploy_cmd.extend(shlex.split(stacks))
    else:
        deploy_cmd.append("--all")

    deploy_cmd.extend(["--context", f"environment={environment}", "--require-approval", "never"])

    run_command(deploy_cmd, env=exec_env)
    print(f"Deployment to {environment} completed successfully!")


def main():
    """Main deployment function."""
    parser = argparse.ArgumentParser(description="Deploy data pipeline CDK stacks")
    parser.add_argument(
        "--environment", "-e", choices=["dev", "staging", "prod"], default="dev", help="Target environment"
    )
    parser.add_argument("--stacks", "-s", help="Specific stacks to deploy (space-separated)")

    args = parser.parse_args()

    # Install dependencies first
    print("Installing Python dependencies...")
    run_command(["pip", "install", "-r", "requirements.txt"])

    # Deploy stacks
    deploy_stacks(args.environment, args.stacks)


if __name__ == "__main__":
    main()
