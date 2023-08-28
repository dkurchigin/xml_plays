import secrets
import zipfile
from io import BytesIO
from multiprocessing import Pool
from timeit import default_timer as timer
from typing import Iterator, List

import pandas
from lxml import etree
from pandas import DataFrame

TOKEN_LENGTH = 64
NAME_LENGTH = 32
OBJECTS_MAX = 10
LEVEL_MAX = 100

XML_COUNT = 100
ZIP_COUNT = 50


def print_duration_time(func):
    def wrapper(*args, **kwargs):
        start = timer()
        func(*args, **kwargs)
        print(f"{func.__name__} - {timer() - start} sec")

    return wrapper


class LXML:
    def __init__(
        self,
        xml_count: int,
        token_length: int,
        level_max: int,
        name_length: int,
        objects_max: int,
    ):
        self.xml_count = xml_count
        self.token_length = token_length
        self.level_max = level_max
        self.name_length = name_length
        self.objects_max = objects_max

    def generate(self) -> Iterator[bytes]:
        for _ in range(self.xml_count):
            root = etree.Element("root")
            objects = etree.Element("objects")
            objects.extend(
                [
                    etree.Element(
                        "object", name=secrets.token_urlsafe(self.name_length)
                    )
                    for _ in range(secrets.randbelow(self.objects_max) + 1)
                ]
            )
            root.extend(
                [
                    etree.Element(
                        "var", name="id", value=secrets.token_hex(self.token_length)
                    ),
                    etree.Element(
                        "var",
                        name="level",
                        value=str(secrets.randbelow(self.level_max) + 1),
                    ),
                    objects,
                ]
            )

            _io = BytesIO()
            tree = etree.ElementTree(root)
            tree.write(_io)
            yield _io.getvalue()

    def read(self, file) -> dict:
        root = etree.parse(file).getroot()
        _id = root.find(".//var[@name='id']")
        level = root.find(".//var[@name='level']")
        objects = root.find("objects")

        return {
            "id": _id.get("value"),
            "level": level.get("value"),
            "object_names": [obj.get("name") for obj in objects.iter("object")],
        }


class CSV:
    @staticmethod
    def parse_level(data) -> DataFrame:
        df = DataFrame.from_dict({"id": [], "level": []})
        for element in data:
            df.loc[len(df)] = [element["id"], element["level"]]
        return df

    @staticmethod
    def parse_objects(data) -> DataFrame:
        df = DataFrame.from_dict({"id": [], "object_name": []})
        for element in data:
            for obj_name in element["object_names"]:
                df.loc[len(df)] = [element["id"], obj_name]
        return df

    @classmethod
    @print_duration_time
    def write(cls, data: List[List[dict]]) -> None:
        with Pool() as p:
            df1 = pandas.concat(p.map(cls.parse_level, data))
            df2 = pandas.concat(p.map(cls.parse_objects, data))

        df1.to_csv("levels.csv", index=False)
        df2.to_csv("objects.csv", index=False)


class Zip:
    def __init__(self, xml: LXML):
        self.xml = xml
        self.generated_files: List[str] = []
        self.loaded_data: List[List[dict]] = []

    def create(self, prefix: int) -> str:
        zip_name = f"{prefix}.zip"
        with zipfile.ZipFile(
            zip_name, mode="w", compression=zipfile.ZIP_DEFLATED
        ) as zf:
            for i, xml_data in enumerate(self.xml.generate()):
                zf.writestr(f"{i}.xml", xml_data)
            return zip_name

    @print_duration_time
    def generate_files(self, count: int) -> None:
        with Pool() as p:
            self.generated_files = p.map(self.create, [i for i in range(count)])

    def read(self, zip_file: str) -> List[dict]:
        res = []
        with zipfile.ZipFile(zip_file) as zf:
            for filename in zf.namelist():
                with zf.open(filename) as f:
                    res.append(self.xml.read(f))
        return res

    @print_duration_time
    def read_generated(self) -> None:
        for filename in self.generated_files:
            self.loaded_data.append(self.read(filename))


if __name__ == "__main__":
    lxml_helper = LXML(
        xml_count=XML_COUNT,
        token_length=TOKEN_LENGTH,
        level_max=LEVEL_MAX,
        name_length=NAME_LENGTH,
        objects_max=OBJECTS_MAX,
    )

    zipper = Zip(lxml_helper)
    zipper.generate_files(ZIP_COUNT)
    zipper.read_generated()
    CSV.write(zipper.loaded_data)
