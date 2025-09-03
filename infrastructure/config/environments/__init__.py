from .dev import dev_config
from .staging import staging_config
from .prod import prod_config


def get_environment_config(environment: str) -> dict:
    """Get configuration for the specified environment."""
    configs = {
        "dev": dev_config,
        "staging": staging_config,
        "prod": prod_config,
    }

    if environment not in configs:
        raise ValueError(f"Unknown environment: {environment}")

    return configs[environment]
