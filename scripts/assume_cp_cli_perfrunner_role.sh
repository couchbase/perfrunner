usage="Usage: source assume_cp_cli_perfrunner_role.sh <cbc-main access key> <cbc-main secret key>"

if [ $# -lt 2 ]; then
    echo "ERROR: not enough arguments provided"
    echo $usage
fi

export AWS_ACCESS_KEY_ID=$1
export AWS_SECRET_ACCESS_KEY=$2

AWS_ASSUMED_ROLE=$(env/bin/aws sts assume-role --role-arn "arn:aws:iam::264138468394:role/_cp-cli-perfrunner" --role-session-name "perfrunner" --duration-seconds 43200)
export AWS_ACCESS_KEY_ID=$(echo "${AWS_ASSUMED_ROLE}" | jq -r .Credentials.AccessKeyId)
export AWS_SECRET_ACCESS_KEY=$(echo "${AWS_ASSUMED_ROLE}" | jq -r .Credentials.SecretAccessKey)
export AWS_SESSION_TOKEN=$(echo "${AWS_ASSUMED_ROLE}" | jq -r .Credentials.SessionToken)
