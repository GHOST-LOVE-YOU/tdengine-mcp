import argparse
import os
import logging
from typing import TypedDict

logger = logging.getLogger(__name__)


class TaosConfig(TypedDict):
    """TDengine configuration"""

    url: str
    username: str
    password: str
    database: str
    timeout: int


def get_taos_config(args: argparse.Namespace) -> TaosConfig:
    """Retrieve the configuration for the TDengine database."""

    logger.debug("Retrieving TDengine configuration...")
    return {
        "url": os.environ.get("TDENGINE_URL", args.taos_url),
        "username": os.environ.get("TDENGINE_USERNAME", args.taos_username),
        "password": os.environ.get("TDENGINE_PASSWORD", args.taos_password),
        "database": os.environ.get("TDENGINE_DATABASE", args.taos_database),
        "timeout": int(os.environ.get("TDENGINE_TIMEOUT", args.taos_timeout)),
    }


def parse_arguments():
    parser = argparse.ArgumentParser(description="TDengine MCP Server")
    parser.add_argument(
        "-url",
        "--taos-url",
        type=str,
        default="http://127.0.0.1:6041",
        help="TDengine URL. Default: `%(default)s`",
    )
    parser.add_argument(
        "-tu",
        "--taos-username",
        type=str,
        default="root",
        help="TDengine username. Default: `%(default)s`",
    )
    parser.add_argument(
        "-pwd",
        "--taos-password",
        type=str,
        default="taosdata",
        help="TDengine password. Default: `%(default)s`",
    )
    parser.add_argument(
        "-db",
        "--taos-database",
        type=str,
        default="default",
        help="TDengine database name. Default: `%(default)s`",
    )
    parser.add_argument(
        "-to",
        "--taos-timeout",
        type=int,
        default=30,
        help="TDengine connection timeout. Default: `%(default)d`",
    )
    parser.add_argument(
        "-ll",
        "--log-level",
        type=str,
        default="INFO",
        help="Log level. Default: `%(default)s`",
    )
    parser.add_argument(
        "-trans",
        "--transport",
        type=str,
        choices=["sse", "stdio"],
        default="sse",
        help="The transport to use. Default: `%(default)s`",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=8000,
        help="Port to listen on for HTTP transports. Default: `%(default)d`",
    )
    parser.add_argument(
        "-H",
        "--host",
        type=str,
        default="0.0.0.0",
        help="Host to listen on for HTTP transports. Default: `%(default)s`",
    )

    return parser.parse_args() 