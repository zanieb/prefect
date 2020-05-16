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
        # for createlabeling job

        super().__init__(**kwargs)

    def _format_labeling_job_alogorithm_specification(region, type):
        return type.format(region)

    def run(
        self,
        labeling_job_name: str,
        label_attribute_name: str,

        # input config
        manifest_s3_uri: dict,
        content_classifiers: Iterable(str) = None,

        #output config
        s3_output_path: str,
        kms_key_id_secret_name: str = None,

        role_arn: str,

        label_category_config_s3_uri: str = None,

        # StoppingConditions
        max_human_labeled_object_count: int = None,
        max_percentage_of_input_dataset_labeled: int = None,

        #LabellingJobAlgorithmsConfig
        # TODO: named tuple or something else specific for below, dict is vague for key requirements
        # must be (type, region)
        labeling_job_algorithm_specification: dict, #TODO: is this actually required?
        initial_active_learning_model_arn: str = None,
        volume_kms_key_id: str = None,

        # TODO: humantaskconfig, can I make this a named tuple or something?
        workteam_arn: str,
        # UI config
        ui_template_s3_uri: str, # TODO: unclear from docs if required
        pre_human_task_lambda_arn: str,  # TODO: should i help them more? is this actually required?
        task_keywords: Iterable[str] = None,
        task_title: str,
        task_description: str,
        number_of_human_workers_per_data_object: int,
        task_time_limit_in_seconds: int,
        task_availability_lifetime_in_seconds: int = None,
        max_concurrent_task_count: int = None,
        # AnnotationConsolidationConfig
        annotation_consolidation_lambda_arn: str,
        # PublicWorkforceTaskPrice
        # AmountinUsd
        dollars: int = None,
        cents: int = None,
        tenth_fractions_of_a_cent: int = None,
        # tags
        tags = Iterable[dict] = None,
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
                "NumberOfHumanWorkersPerDataObject": number_of_human_workers_per_data_object,
                "TaskTimeLimitInSeconds": task_time_limit_in_seconds,
                "TaskAvailabilityLifetimeInSeconds": task_availability_lifetime_in_seconds,
                "MaxConcurrentTaskCount": max_concurrent_task_count,
                "AnnotationConsolidationConfig": {
                    "AnnotationConsolidationLambdaArn": annotation_consolidation_lambda_arn
                },
                "PublicWorkforceTaskPrice": {
                    "AmountInUsd": {
                        "Dollars": dollars,
                        "Cents": cents,
                        "TenthFractionsOfACent": tenth_fractions_of_a_cent
                    }
                }
            },
            Tags=tags
        )

        # convert the JSON I got into a dictionary
        result = json.loads(json_response)

        return result
