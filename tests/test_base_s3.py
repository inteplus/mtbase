import asyncio
import sys
import unittest
from datetime import datetime, timedelta, timezone

sys.path.insert(0, "/home/minhtri/gitcentre/mtbase")

import mt.base

assert mt.base.__file__.startswith("/home/minhtri/gitcentre/mtbase/"), mt.base.__file__

try:
    import boto3
    from botocore.stub import Stubber

    HAVE_BOTO = True
except ImportError:
    HAVE_BOTO = False

from mt.base import s3


@unittest.skipUnless(HAVE_BOTO, "boto3 not installed")
class TestListRecentObjects(unittest.TestCase):
    def _client(self):
        return boto3.client(
            "s3",
            region_name="us-east-1",
            aws_access_key_id="x",
            aws_secret_access_key="y",
        )

    def test_returns_only_recent_objects(self):
        t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        objs = [
            {"Key": "p/old", "LastModified": t0, "Size": 1},
            {"Key": "p/new1", "LastModified": t0 + timedelta(days=10), "Size": 2},
            {"Key": "p/new2", "LastModified": t0 + timedelta(days=20), "Size": 3},
        ]
        client = self._client()
        with Stubber(client) as stubber:
            stubber.add_response(
                "list_objects_v2",
                {"Contents": objs, "IsTruncated": False, "KeyCount": 3},
                {"Bucket": "b", "Prefix": "p"},
            )
            cv = {"async": False, "s3_client": client}
            res = asyncio.run(
                s3.list_recent_objects("s3://b/p", t0 + timedelta(days=5), context_vars=cv)
            )
        self.assertEqual([o["Key"] for o in res], ["p/new1", "p/new2"])

    def test_no_match_and_empty_bucket(self):
        t0 = datetime(2024, 1, 1, tzinfo=timezone.utc)
        client = self._client()
        with Stubber(client) as stubber:
            stubber.add_response(
                "list_objects_v2",
                {
                    "Contents": [{"Key": "p/old", "LastModified": t0}],
                    "IsTruncated": False,
                    "KeyCount": 1,
                },
                {"Bucket": "b", "Prefix": "p"},
            )
            stubber.add_response(
                "list_objects_v2",
                {"IsTruncated": False, "KeyCount": 0},
                {"Bucket": "b", "Prefix": "p"},
            )
            cv = {"async": False, "s3_client": client}
            min_dt = t0 + timedelta(days=1)
            self.assertEqual(
                asyncio.run(s3.list_recent_objects("s3://b/p", min_dt, context_vars=cv)), []
            )
            self.assertEqual(
                asyncio.run(s3.list_recent_objects("s3://b/p", min_dt, context_vars=cv)), []
            )


if __name__ == "__main__":
    unittest.main()
