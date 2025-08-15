import json, argparse
from dataclasses import dataclass
from typing import List, Optional

MAX_COUNT_IN_A_CALL = 1000


def parse_args():
    parser = argparse.ArgumentParser(description="A simple argument parser.")
    parser.add_argument(
        "-c",
        "--config",
        type=str,
        default="instance-region.cfg",
        help="configuration file",
    )
    parser.add_argument("-k", "--key", type=str, default="yuanl", help="key name id")
    parser.add_argument(
        "-r", "--role", type=str, default="yuanl_exp_slave", help="role"
    )
    parser.add_argument("-s", "--sample", action="store_true", help="sample")
    args = parser.parse_args()
    return args


@dataclass
class Instance:
    instanceId: str
    publicIpAddress: str


@dataclass
class Zone:
    name: str
    subnet: Optional[str] = None


@dataclass
class Region:
    name: str
    image: str
    security_group_id: str
    count: int
    zones: List[Zone]


@dataclass
class InstanceType:
    name: str
    nodes: int


@dataclass
class CloudConfig:
    access_key_id: str
    access_key_secret: str
    regions: List[Region]
    type: List[InstanceType]


@dataclass
class Config:
    aliyun: List[CloudConfig]
    aws: List[CloudConfig]


def from_dict(data: dict) -> CloudConfig:
    regions = [
        Region(
            name=r["name"],
            image=r["image"],
            security_group_id=r["security_group_id"],
            count=r["count"],
            zones=[Zone(**z) for z in r.get("zones", [])],
        )
        for r in data["regions"]
    ]

    types = [InstanceType(**t) for t in data["type"]]

    return CloudConfig(
        access_key_id=data["aws_access_key_id"],
        access_key_secret=data["aws_secret_access_key"],
        regions=regions,
        type=types,
    )


def parse_zone(d: dict) -> Zone:
    return Zone(name=d["name"], subnet=d.get("subnet"))


def parse_region(d: dict) -> Region:
    return Region(
        name=d["name"],
        image=d.get("image", ""),
        count=d.get("count", 0),
        security_group_id=d.get("security_group_id", ""),
        zones=[parse_zone(z) for z in d.get("zones", [])],
    )


def parse_instance_type(d: dict) -> InstanceType:
    return InstanceType(name=d["name"], nodes=d.get("nodes", 0))


def parse_cloud_config(d: dict) -> CloudConfig:
    return CloudConfig(
        access_key_id=d.get("access_key_id", ""),
        access_key_secret=d.get("access_key_secret", ""),
        regions=[parse_region(r) for r in d.get("regions", [])],
        type=[parse_instance_type(t) for t in d.get("type", [])],
    )


def load_config(path: str) -> Config:
    with open(path, "r", encoding="utf-8") as f:
        raw = json.load(f)

    return Config(
        aliyun=[parse_cloud_config(a) for a in raw.get("aliyun", [])],
        aws=[parse_cloud_config(a) for a in raw.get("aws", [])],
    )
