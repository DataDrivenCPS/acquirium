from unittest.mock import Mock, patch

import pytest

from acquirium.Materialization.effects import EffectIntent
from acquirium.Server.effect_worker import deliver_effect


def test_webhook_delivery_uses_effect_idempotency_key():
    intent = EffectIntent("effect", "execution", "webhook", "https://example.test/hook",
                          {"message": "updated", "headers": {"X-Source": "dashboard"}}, "dedupe-key")
    response = Mock()
    with patch("acquirium.Server.effect_worker.requests.post", return_value=response) as post:
        deliver_effect(intent)
    post.assert_called_once_with("https://example.test/hook", json={"message": "updated"},
                                 headers={"X-Source": "dashboard", "Idempotency-Key": "dedupe-key"}, timeout=5.0)
    response.raise_for_status.assert_called_once_with()


def test_effect_delivery_rejects_non_http_destination():
    intent = EffectIntent("effect", "execution", "webhook", "file:///tmp/out", {}, "dedupe-key")
    with pytest.raises(ValueError, match="absolute http"):
        deliver_effect(intent)
