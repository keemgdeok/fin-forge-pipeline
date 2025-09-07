from __future__ import annotations

from typing import Any, Dict, List, Optional
from botocore.exceptions import ClientError


class S3Stub:
    def __init__(self, *, keycount: int = 0, head_ok: bool = True) -> None:
        self.keycount = keycount
        self.head_ok = head_ok
        self.put_calls: List[Dict[str, Any]] = []

    def list_objects_v2(self, **kwargs: Any) -> Dict[str, Any]:
        return {"KeyCount": self.keycount}

    def put_object(self, **kwargs: Any) -> Dict[str, Any]:
        self.put_calls.append(kwargs)
        return {"ETag": "stub"}

    def head_object(self, **kwargs: Any) -> Dict[str, Any]:
        if self.head_ok:
            return {"ResponseMetadata": {"HTTPStatusCode": 200}}
        raise ClientError({"Error": {"Code": "404", "Message": "Not Found"}}, "HeadObject")


class SQSStub:
    def __init__(self) -> None:
        self.batches: List[List[Dict[str, Any]]] = []

    def send_message_batch(self, **kwargs: Any) -> Dict[str, Any]:
        entries = kwargs.get("Entries", [])
        self.batches.append(entries)
        return {"Successful": [{"Id": e.get("Id", str(i))} for i, e in enumerate(entries)]}


class SnsStub:
    def __init__(self) -> None:
        self.published: List[Dict[str, Any]] = []

    def publish(self, **kwargs: Any) -> Dict[str, Any]:
        self.published.append(kwargs)
        return {"MessageId": "mid-123"}


class CloudWatchStub:
    def __init__(self) -> None:
        self.metrics: List[Dict[str, Any]] = []

    def put_metric_data(self, **kwargs: Any) -> Dict[str, Any]:
        self.metrics.append(kwargs)
        return {}


class StepFunctionsStub:
    def start_execution(self, **kwargs: Any) -> Dict[str, Any]:
        return {"executionArn": "arn:states:stub"}


class SesStub:
    def __init__(self) -> None:
        self.sent: List[Dict[str, Any]] = []

    def send_email(self, **kwargs: Any) -> Dict[str, Any]:
        self.sent.append(kwargs)
        return {"MessageId": "em-1"}


class BotoStub:
    def __init__(
        self,
        *,
        s3: Optional[Any] = None,
        sqs: Optional[Any] = None,
        sns: Optional[Any] = None,
        cloudwatch: Optional[Any] = None,
        stepfunctions: Optional[Any] = None,
        ssm: Optional[Any] = None,
        ses: Optional[Any] = None,
    ) -> None:
        self._s3 = s3
        self._sqs = sqs
        self._sns = sns
        self._cw = cloudwatch
        self._sfn = stepfunctions
        self._ssm = ssm
        self._ses = ses

    def client(self, name: str, **kwargs: Any) -> Any:
        if name == "s3" and self._s3 is not None:
            return self._s3
        if name == "sqs" and self._sqs is not None:
            return self._sqs
        if name == "sns" and self._sns is not None:
            return self._sns
        if name == "cloudwatch" and self._cw is not None:
            return self._cw
        if name == "stepfunctions" and self._sfn is not None:
            return self._sfn
        if name == "ssm" and self._ssm is not None:
            return self._ssm
        if name == "ses" and self._ses is not None:
            return self._ses

        # Provide minimal stub for unknown client names
        class _Stub:
            pass

        return _Stub()
