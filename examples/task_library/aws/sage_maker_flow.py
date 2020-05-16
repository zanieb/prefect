from prefect import Flow
from prefect.tasks.aws.sage_maker import CreateLabelingJob

label_job = CreateLabelingJob()

with Flow('example SageMaker flow') as flow:
    labels = label_job()