"""
setup_iam.py
------------
Creates the IAM user, group, and policy needed for the Football-DE project.

Resources created:
  - Group  : football-de-team
  - Policy : FootballDES3Policy  (read/write on football-de-2026 bucket)
  - User   : football-de-user    (added to the group)
"""

import json
import boto3
from botocore.exceptions import ClientError

# ── Config ────────────────────────────────────────────────────────────────────
BUCKET      = "football-de-2026"
GROUP_NAME  = "football-de-team"
POLICY_NAME = "FootballDES3Policy"
USER_NAME   = "football-de-user"
# ─────────────────────────────────────────────────────────────────────────────

POLICY_DOCUMENT = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "S3BronzeSilverAccess",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:ListBucket",
            ],
            "Resource": [
                f"arn:aws:s3:::{BUCKET}",
                f"arn:aws:s3:::{BUCKET}/*",
            ],
        }
    ],
}


def get_account_id(iam) -> str:
    sts = boto3.client("sts")
    return sts.get_caller_identity()["Account"]


def create_policy(iam, account_id: str) -> str:
    policy_arn = f"arn:aws:iam::{account_id}:policy/{POLICY_NAME}"
    try:
        response = iam.create_policy(
            PolicyName=POLICY_NAME,
            PolicyDocument=json.dumps(POLICY_DOCUMENT),
            Description="R/W access to football-de-2026 S3 bucket (bronze + silver)",
        )
        arn = response["Policy"]["Arn"]
        print(f"  ✅  Policy created : {arn}")
        return arn
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            print(f"  ℹ️   Policy already exists : {policy_arn}")
            return policy_arn
        raise


def create_group(iam, policy_arn: str) -> None:
    try:
        iam.create_group(GroupName=GROUP_NAME)
        print(f"  ✅  Group created  : {GROUP_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            print(f"  ℹ️   Group already exists : {GROUP_NAME}")
        else:
            raise

    iam.attach_group_policy(GroupName=GROUP_NAME, PolicyArn=policy_arn)
    print(f"  ✅  Policy attached to group")


def create_user(iam) -> None:
    try:
        iam.create_user(UserName=USER_NAME)
        print(f"  ✅  User created   : {USER_NAME}")
    except ClientError as e:
        if e.response["Error"]["Code"] == "EntityAlreadyExists":
            print(f"  ℹ️   User already exists : {USER_NAME}")
        else:
            raise

    iam.add_user_to_group(GroupName=GROUP_NAME, UserName=USER_NAME)
    print(f"  ✅  User added to group : {GROUP_NAME}")


def main() -> None:
    print("\n🔐  Setting up IAM for Football-DE project\n")
    iam        = boto3.client("iam")
    account_id = get_account_id(iam)

    policy_arn = create_policy(iam, account_id)
    create_group(iam, policy_arn)
    create_user(iam)

    print("\n  Summary")
    print(f"  {'─'*40}")
    print(f"  Group  : {GROUP_NAME}")
    print(f"  Policy : {POLICY_NAME}")
    print(f"  User   : {USER_NAME}")
    print(f"\n✅  IAM setup complete.\n")


if __name__ == "__main__":
    main()
