from unittest.mock import MagicMock, patch


def test_upload_to_s3_calls_put_object():
    fake_s3 = MagicMock()
    fake_s3.put_object.return_value = {}

    with patch("include.scripts.fetch_ogd.S3_BUCKET", "test-bucket"):
        with patch("include.scripts.fetch_ogd.boto3.client", return_value=fake_s3):
            from include.scripts.fetch_ogd import upload_to_s3

            key = upload_to_s3([{"city": "Delhi"}])

    assert key.startswith("raw/ogd/year=")
    fake_s3.put_object.assert_called_once()
