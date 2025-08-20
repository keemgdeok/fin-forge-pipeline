# GitHub Actions CI/CD Setup

This document explains how to set up and configure GitHub Actions for automated deployment of the data pipeline.

## 🔧 Setup Requirements

### 1. GitHub Repository Secrets

Add the following secrets to your GitHub repository (`Settings > Secrets and variables > Actions`):

#### Development Environment:
- `AWS_ACCESS_KEY_ID` - AWS Access Key for development account
- `AWS_SECRET_ACCESS_KEY` - AWS Secret Key for development account

#### Production Environment:
- `AWS_ACCESS_KEY_ID_PROD` - AWS Access Key for production account  
- `AWS_SECRET_ACCESS_KEY_PROD` - AWS Secret Key for production account

### 2. GitHub Environments

Create the following environments in your repository (`Settings > Environments`):

#### Development Environment:
- **Name**: `development`
- **Protection rules**: None (auto-deploy)
- **Environment secrets**: Use repository secrets

#### Production Environment:
- **Name**: `production`
- **Protection rules**: 
  - ✅ Required reviewers (1-2 people)
  - ✅ Wait timer: 5 minutes
- **Environment secrets**: Use production-specific secrets

## 📋 Workflow Overview

### 1. **Unit Tests** (`unit-tests.yml`)
**Triggers:** Every push and PR
- Runs tests on Python 3.10, 3.11, 3.12
- Code quality checks (black, flake8, mypy)
- Security scanning (bandit, safety)
- Coverage reporting

### 2. **CDK Diff** (`cdk-diff.yml`)
**Triggers:** PRs that modify infrastructure
- Shows CDK changes before deployment
- Security analysis
- Comments results on PR automatically

### 3. **Development Deployment** (`cdk-deploy-dev.yml`)
**Triggers:** Push to `main` or `develop` branch
- Runs unit tests first
- Deploys to development environment
- Automatic deployment (no approval needed)

### 4. **Production Deployment** (`cdk-deploy-prod.yml`)
**Triggers:** 
- Git tags starting with `v*` (e.g., `v1.0.0`)
- Manual workflow dispatch (with confirmation)

## 🚀 Deployment Process

### Development Deployment
```bash
# Automatically triggers on push to main/develop
git push origin main
```

### Production Deployment

#### Option 1: Tag-based deployment
```bash
# Create and push a release tag
git tag v1.0.0
git push origin v1.0.0
```

#### Option 2: Manual deployment
1. Go to **Actions** tab in GitHub
2. Select **"Deploy to Production"** workflow
3. Click **"Run workflow"**
4. Type `DEPLOY_TO_PRODUCTION` in the confirmation field
5. Click **"Run workflow"**

## 🔍 Monitoring Deployments

### Viewing Deployment Status
- **Actions Tab**: See all workflow runs
- **Environments**: Check deployment history and status
- **Artifacts**: Download CDK outputs and reports

### Deployment Notifications
Configure notifications in `Settings > Notifications` to get alerts for:
- Failed deployments
- Successful production deployments
- PR reviews required

## 🛡️ Security Features

### Automated Security Checks
- **Bandit**: Python security linting
- **Safety**: Check for known security vulnerabilities
- **CDK Security Analysis**: Detect potential public access issues

### Production Safeguards
- **Required Approvals**: Manual review before production deployment
- **Wait Timer**: 5-minute cooling period
- **Confirmation Required**: Must type confirmation text for manual deployments
- **Separate Credentials**: Different AWS credentials for prod

## 🔧 Customization

### Modifying Deployment Triggers
Edit the `on:` section in workflow files:

```yaml
# Deploy only on specific branches
on:
  push:
    branches: [main]

# Deploy on tags matching pattern
on:
  push:
    tags: ['v*.*.*']
```

### Adding New Environments
1. Create new environment in GitHub settings
2. Copy existing workflow file
3. Update environment name and secrets
4. Modify CDK context parameters

### Custom Deployment Steps
Add steps before/after CDK deployment:

```yaml
- name: Custom pre-deployment step
  run: |
    echo "Running custom validation..."
    python scripts/validate_deployment.py

- name: CDK Deploy
  run: |
    cdk deploy --all --context environment=prod

- name: Custom post-deployment step  
  run: |
    echo "Running post-deployment tests..."
    python scripts/integration_tests.py
```

## 📊 Best Practices

### Branch Strategy
- **`develop`**: Development work and testing
- **`main`**: Stable code, auto-deploys to dev
- **`v*` tags**: Production releases

### Deployment Safety
- Always review CDK diff before merging PRs
- Use feature branches for infrastructure changes
- Test changes in development first
- Schedule production deployments during maintenance windows

### Monitoring
- Set up CloudWatch alarms for deployment failures
- Monitor AWS costs after deployments
- Use CDK outputs to verify successful deployment

## 🆘 Troubleshooting

### Common Issues

#### Authentication Failures
```
Error: Need to perform AWS calls but no credentials found
```
**Solution**: Check AWS secrets in GitHub repository settings

#### CDK Bootstrap Required
```
Error: This stack uses assets, so the toolkit stack must be deployed
```
**Solution**: Bootstrap runs automatically, but you can manually run:
```bash
cdk bootstrap --context environment=dev
```

#### Deployment Timeouts
```
Error: Resource creation cancelled
```
**Solution**: Check CloudFormation console for detailed error messages

### Getting Help
- Check workflow logs in GitHub Actions
- Review CloudFormation events in AWS Console
- Check CDK diff output for understanding changes
- Contact team leads for production deployment issues