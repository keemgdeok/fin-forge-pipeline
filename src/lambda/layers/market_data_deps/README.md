Market Data Dependencies Lambda Layer
=====================================

Purpose
- This Lambda layer packages third-party libraries required for fetching market data (e.g., Yahoo Finance) without inflating the Lambda function bundle (avoids the Fat Lambda anti-pattern).

Layout
- Standard Lambda layer structure: place all site-packages under `python/`.

Build locally
- Install dependencies into the `python/` folder so that CDK `Code.from_asset` zips the layer with dependencies.

Commands
```bash
# From repository root
pip install -r src/lambda/layers/market_data_deps/requirements.txt \
    -t src/lambda/layers/market_data_deps/python
```

Notes
- Keep heavy scientific packages in this dedicated layer rather than in the function.
- If deploying in CI, run the above in a Linux x86_64 environment to ensure manylinux wheels compatible with AWS Lambda.
- You can also use a Docker build step with the public Lambda base image for reproducible builds.

