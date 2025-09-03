#!/usr/bin/env python3
"""Deployment script for the data pipeline CDK application."""
import argparse
import subprocess
import sys
import os
from typing import Optional


def run_command(command: str, check: bool = True) -> subprocess.CompletedProcess:
    """Run a shell command and return the result."""
    print(f"Running: {command}")
    result = subprocess.run(command, shell=True, capture_output=True, text=True)

    if check and result.returncode != 0:
        print(f"Command failed with return code {result.returncode}")
        print(f"stdout: {result.stdout}")
        print(f"stderr: {result.stderr}")
        sys.exit(1)

    return result


def deploy_stacks(environment: str, stacks: Optional[str] = None) -> None:
    """Deploy CDK stacks to the specified environment."""
    print(f"Deploying to environment: {environment}")

    # Set environment context
    os.environ["CDK_DEFAULT_REGION"] = "us-east-1"

    # Bootstrap CDK if needed
    print("Checking CDK bootstrap status...")
    bootstrap_cmd = f"cdk bootstrap --context environment={environment}"
    run_command(bootstrap_cmd, check=False)

    # Deploy stacks
    if stacks:
        deploy_cmd = f"cdk deploy {stacks} --context environment={environment} --require-approval never"
    else:
        deploy_cmd = f"cdk deploy --all --context environment={environment} --require-approval never"

    run_command(deploy_cmd)
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
    run_command("pip install -r requirements.txt")

    # Deploy stacks
    deploy_stacks(args.environment, args.stacks)


if __name__ == "__main__":
    main()
