import boto3

def get_boto_session(region_name, profile_name="803461793753_Project_Administrator"):
    return boto3.session.Session(profile_name=profile_name, region_name=region_name)
