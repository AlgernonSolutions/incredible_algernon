(
    sam validate --profile dev
)&&(
    sam build ^
        --profile dev ^
        --use-container -b .aws-sam\build
)&&(
    sam package ^
        --s3-bucket algernonsolutions-layer-dev ^
        --template-file .aws-sam\build\template.yaml ^
        --profile dev ^
        --output-template-file .aws-sam\build\templated.yaml
)&&(
    aws cloudformation deploy ^
        --profile dev ^
        --template .aws-sam\build\templated.yaml ^
        --stack-name incredible-dev ^
        --capabilities CAPABILITY_AUTO_EXPAND CAPABILITY_NAMED_IAM ^
        --force-upload
)