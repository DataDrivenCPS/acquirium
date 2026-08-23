"""Logging configuration contracts."""


def test_verbose_debugs_acquirium_tree_only():
    import logging

    from acquirium.internals._log import configure_logging

    configure_logging(verbose=True)
    try:
        assert logging.getLogger("acquirium").level == logging.DEBUG
        assert logging.getLogger().level == logging.INFO
        assert not logging.getLogger("pyomo").isEnabledFor(logging.DEBUG)
        assert logging.getLogger("acquirium.graph_store").isEnabledFor(logging.DEBUG)
    finally:
        configure_logging(verbose=False)
