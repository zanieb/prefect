import boto3
import json

from prefect import Task


IMAGE_CLASSIFICATION = "arn:aws:sagemaker:{}:027400017018:labeling-job-algorithm-specification/image-classification"
TEXT_CLASSIFICATION = "arn:aws:sagemaker:{}:027400017018:labeling-job-algorithm-specification/text-classification"
OBJECT_DETECTION = "arn:aws:sagemaker:{}:027400017018:labeling-job-algorithm-specification/object-detection"
SEMANTIC_SEGMENTATION = "arn:aws:sagemaker:{}:027400017018:labeling-job-algorithm-specification/semantic-segmentation"


class CreateLabelingJob(Task):
    def __init__(self, **kwargs):
        # do stuff
        # that is special
        # for createlabeliing job

        super().__init__(**kwargs)

    def _format_labeling_job_alogorithm_specification(region, type):
        return type.format(region)

    def run(
        self,
        labeling_job_name: str,
        label_attribute_name: str,
        manifest_s3_uri: dict,
        content_classifiers: Iterable(str),
        s3_output_path: str,
        kms_key_id_secret_name: str,
        role_arn: str,
        label_category_config_s3_uri: str,
        max_human_labeled_object_count: int,
        max_percentage_of_input_dataset_labeled: int,
        labeling_job_algorithm_specification: dict,
        initial_active_learning_model_arn: str,
        volume_kms_key_id: str,
        workteam_arn: str,
        ui_template_s3_uri: str,
        pre_human_task_lambda_arn: str,  # todo: should i help them more??????????
        task_keywords: Iterable[str],
        task_title: str,
        **kwargs
    ) -> dict:
        # actually be able to do the thing
        # in my case, it needs to be able to

        # instantiate a boto3 client
        client = boto3.client("sagemaker")

        # create a labeling job with a s3 uri
        json_response = client.create_labeling_job(
            LabelingJobName=labeling_job_name,
            LabelAttributeName=label_attribute_name,
            InputConfig={
                "DataSource": {"S3DataSource": {"ManifestS3Uri": manifest_s3_uri}},
                "DataAttributes": {"ContentClassifiers": content_classifiers},
            },
            OutputConfig={
                "S3OutputPath": s3_output_path,
                "KmsKeyId": kms_key_id,  # todo: double check is this really a name not a secret value
            },
            RoleArn=role_arn,
            LabelCategoryConfigS3Uri=label_category_config_s3_uri,
            StoppingConditions={
                "MaxHumanLabeledObjectCount": max_human_labeled_object_count,
                "MaxPercentageOfInputDatasetLabeled": max_percentage_of_input_dataset_labeled,
            },
            LabelingJobAlgorithmsConfig={
                "LabelingJobAlgorithmSpecificationArn": _format_labeling_job_alogorithm_specification(
                    **labeling_job_algorithm_specification
                ),
                "InitialActiveLearningModelArn": initial_active_learning_model_arn,
                "LabelingJobResourceConfig": {"VolumeKmsKeyId": volume_kms_key_id},
            },
            HumanTaskConfig={
                "WorkTeamArn": workteam_arn,
                "UiConfig": {"UiTemplateS3Uri": ui_template_s3_uri},
                "PreHumanTaskLambdaArn": pre_human_task_lambda_arn,
                "TaskKeywords": task_keywords,
                "TaskTitle": task_title,
                "TaskDescription": task_description,
                "NumberOfHumanWorkersPerDataObject": "hi",
            },
        )

        # convert the JSON I got into a dictionary
        result = json.loads(json_response)

        return result
