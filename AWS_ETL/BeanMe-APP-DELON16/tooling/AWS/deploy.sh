#!/usr/bin/env bash
set -eu

###
### Script to deploy S3 bucket, lambda and EC2 in CloudFormation stack
###

#### CONFIGURATION SECTION ####

if [ "$#" -ne 3 ]; then
  echo "Usage: $0 <aws_profile> <team_name> <ec2_ingress_ip>"
  exit 1
fi

# Variables
aws_profile="${1:-de-course}"  # Default: de-course
team_name="${2:-beanmeapp}"    # Default: beanmeapp
ec2_ingress_ip="${3:-$(curl -s https://checkip.amazonaws.com)/32}" # Default: {your IP}

# Directories
root_dir="$(cd "$(dirname "$0")/../.." && pwd)" # Project root
lambda_src_dir="${root_dir}/src"
build_dir="${root_dir}/build"
deps_dir="${build_dir}/deps"

# Constants
region="eu-west-1"
deployment_bucket="${team_name}-deployment-bucket"
userdata_file="userdata"
etl_template="etl-stack.yaml"
etl_output_template="etl-stack-packaged.yaml"
requirements_file="requirements-lambda.txt"

# Validate required files exist
for f in "$userdata_file" "$etl_template" "$requirements_file" "deployment-bucket-stack.yaml"; do
  if [ ! -f "$f" ]; then
    echo "Error: Required file '$f' not found!"
    exit 1
  fi
done

# Check base64 is available
if ! command -v base64 >/dev/null 2>&1; then
  echo "Error: 'base64' command not found."
  exit 1
fi

# Check AWS CLI
if ! command -v aws >/dev/null 2>&1; then
  echo "Error: AWS CLI not installed or not in PATH."
  exit 1
fi

# Base64 encode user data
echo "Encoding EC2 user data..."
ec2_userdata=$(base64 -i "$userdata_file")

#### DEPLOYMENT SECTION ####

# Create deployment bucket
echo ""
echo "Creating deployment bucket stack..."
aws cloudformation deploy --stack-name "${team_name}-deployment-bucket" \
  --template-file deployment-bucket-stack.yaml \
  --region "$region" \
  --capabilities CAPABILITY_IAM \
  --profile "$aws_profile"

# Wait for deployment bucket to complete
echo "Waiting for deployment bucket stack to complete..."
aws cloudformation wait stack-create-complete \
  --stack-name "${team_name}-deployment-bucket" \
  --region "$region" \
  --profile "$aws_profile"

# Create build directories
mkdir -p "$build_dir"
mkdir -p "$deps_dir"

# Download dependencies
if [ -z "${SKIP_PIP_INSTALL:-}" ]; then
  echo ""
  echo "Installing Python dependencies for Lambda..."
  echo ""
  py -m pip install --platform manylinux2014_x86_64 \
    --target="$deps_dir" --implementation cp --python-version 3.12 \
    --only-binary=:all: --upgrade -r "$requirements_file"
else
  echo "Skipping pip install due to SKIP_PIP_INSTALL=true"
fi

# Package Lambda functions
echo ""
echo "Packaging Lambda functions..."
echo ""

package_lambda() {
  local name="$1"
  local lambda_dir="${build_dir}/${name}"
  local zip_file="${build_dir}/${name}.zip"

  echo " → Packaging ${name}.py"
  mkdir -p "$lambda_dir"
  cp -r "${lambda_src_dir}/${name}/." "$lambda_dir/"

  # Only include dependencies if packaging the 'load' or 'create_table' Lambda
  if [[ "$name" == "load" || "$name" == "create_tables" ]]; then
    echo "   ↳ Including dependencies from requirements-lambda.txt"
    cp -r "${deps_dir}/." "$lambda_dir/"
  else
    echo "   ↳ Skipping dependency bundling for ${name}"
  fi

  # Convert path to Windows-style path for PowerShell compatibility
  local windows_lambda_dir=$(cygpath -w "$lambda_dir")
  local windows_zip_file=$(cygpath -w "$zip_file")

  # Compress the files
  powershell.exe -NoProfile -Command \
    "Compress-Archive -Path '${windows_lambda_dir}\\*' -DestinationPath '${windows_zip_file}'"

  # Upload files to S3 bucket
  aws s3 cp "$zip_file" "s3://${deployment_bucket}/" --profile "$aws_profile"
}

# Functions to package
package_lambda extract
package_lambda transform
package_lambda load
package_lambda create_tables

# Create cloudformation template
echo ""
echo "Packaging CloudFormation template..."
aws cloudformation package --template-file "$etl_template" \
  --s3-bucket "$deployment_bucket" \
  --output-template-file "$etl_output_template" \
  --profile "$aws_profile"

echo ""

# Deploy ETL stack
echo "Deploying ETL pipeline stack..."
aws cloudformation deploy --stack-name "${team_name}-etl-pipeline" \
  --template-file "$etl_output_template" \
  --region "$region" \
  --capabilities CAPABILITY_IAM \
  --capabilities CAPABILITY_NAMED_IAM \
  --profile "$aws_profile" \
  --parameter-overrides \
    TeamName="$team_name" \
    EC2InstanceIngressIp="$ec2_ingress_ip" \
    EC2UserData="$ec2_userdata"

echo ""
echo "Deployment complete."

#### CLEANUP SECTION ####

echo ""
echo "Cleaning up..."
echo ""

rm -r ${build_dir}
rm etl-stack-packaged.yaml

aws s3 rm "s3://${deployment_bucket}" --recursive --profile "$aws_profile"

aws cloudformation delete-stack \
  --stack-name "${team_name}-deployment-bucket" \
  --region "$region" \
  --profile "$aws_profile"

echo ""
echo "Clean up complete."
