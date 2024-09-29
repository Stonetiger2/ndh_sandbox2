import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

args = getResolvedOptions(sys.argv, ['JOB_NAME'])
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

# Script generated for node S3 bucket
S3bucket_node1 = glueContext.create_dynamic_frame.from_options(format_options={"quoteChar": "\"", "withHeader": True, "separator": ",", "optimizePerformance": False}, connection_type="s3", format="csv", connection_options={"paths": ["s3://rwang-datalake-bronze/pbp_submissions_csv/submissions-utf8.csv"], "recurse": True}, transformation_ctx="S3bucket_node1")

# Script generated for node Change Schema
# Hello
ChangeSchema_node2 = ApplyMapping.apply(frame=S3bucket_node1, mappings=[("pbp_a_hnumber", "string", "pbp_a_hnumber", "string"), ("pbp_a_plan_identifier", "string", "pbp_a_plan_identifier", "string"), ("segment_id", "string", "segment_id", "string"), ("pbp_a_ben_cov", "string", "pbp_a_ben_cov", "string"), ("pbp_a_plan_type", "string", "pbp_a_plan_type", "string"), ("orgtype", "string", "orgtype", "string"), ("bid_id", "string", "bid_id", "string"), ("version", "string", "version", "string"), ("plan_status", "string", "plan_status", "string"), ("eghp_plan_flag", "string", "eghp_plan_flag", "string"), ("specialty_plan_flag", "string", "specialty_plan_flag", "string"), ("specialty_plan_type", "string", "specialty_plan_type", "string"), ("specialty_plan_redesig_date", "string", "specialty_plan_redesig_date", "string"), ("acrp_version", "string", "acrp_version", "string"), ("pending_contract_flag", "string", "pending_contract_flag", "string"), ("formulary_id", "string", "formulary_id", "string"), ("pace_population", "string", "pace_population", "string"), ("range_id", "string", "range_id", "string"), ("employer_bid", "string", "employer_bid", "string"), ("only_800series_flag", "string", "only_800series_flag", "string"), ("parent_organization", "string", "parent_organization", "string"), ("schmo_type", "string", "schmo_type", "string"), ("demo_flag", "string", "demo_flag", "string"), ("effective_date", "string", "effective_date", "string"), ("organization_marketing_name", "string", "organization_marketing_name", "string"), ("plan_name", "string", "plan_name", "string"), ("cms_approval_date", "string", "cms_approval_date", "string"), ("submission_type", "string", "submission_type", "string"), ("fide_snp", "string", "fide_snp", "string"), ("highquality_snp", "string", "highquality_snp", "string"), ("vbid_flag", "string", "vbid_flag", "string"), ("vbid_partc_flag", "string", "vbid_partc_flag", "string"), ("vbid_partd_flag", "string", "vbid_partd_flag", "string"), ("vbid_uf_flag", "string", "vbid_uf_flag", "string"), ("mtm_flag", "string", "mtm_flag", "string"), ("part_d_model_demo", "string", "part_d_model_demo", "string"), ("part_d_enhncd_cvrg_demo", "string", "part_d_enhncd_cvrg_demo", "string"), ("manual_change_version", "string", "manual_change_version", "string"), ("manual_change_date", "string", "manual_change_date", "string")], transformation_ctx="ChangeSchema_node2")

# Script generated for node S3 bucket
S3bucket_node3 = glueContext.getSink(path="s3://rwang-datalake-silver/pbp_submissions/", connection_type="s3", updateBehavior="UPDATE_IN_DATABASE", partitionKeys=[], enableUpdateCatalog=True, transformation_ctx="S3bucket_node3")
S3bucket_node3.setCatalogInfo(catalogDatabase="lake-silver",catalogTableName="pbp_submissions")
S3bucket_node3.setFormat("glueparquet", compression="snappy")
S3bucket_node3.writeFrame(ChangeSchema_node2)
job.commit()