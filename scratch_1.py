from prefect import Flow
from prefect.tasks.aws.s3 import S3Upload
from prefect.tasks.aws.sage_maker import CreateLabelingJob

# etl

s3_upload = S3Upload()

create_labeling_job = CreateLabelingJon()


with Flow("example flow") as f:
    # pretend an ETL happened

    uri = s3_upload()
    create_labeling_job(
        manifest_s3_uri=uri,
        kms_key_id="my_aws_key",
        labeling_job_algorithm_specification={
            "region": "us-east-1",
            "type": sage_maker.algorithms.IMAGE_CLASSIFICATION,
        },
    )
