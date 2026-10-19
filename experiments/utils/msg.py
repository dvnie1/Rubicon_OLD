
from __future__ import annotations

from collections.abc import Generator
from proto.UnitDefinition_pb2 import TcpPackageType
from proto.UnitDefinition_pb2 import UnitType

class TCPMessage:
    delim: int = 0x5ADB0BB1
    HEADER_SIZE: int = 40  # 4+4+4+4+8+8+8

    @classmethod
    def parse_from_bytes(cls, data: bytes) -> Generator[TCPMessage, None, None]:
        pos = 0
        while pos < len(data):
            if pos + 4 > len(data):
                break

            delim = int.from_bytes(data[pos:pos+4], byteorder='little')
            if delim != TCPMessage.delim:
                raise ValueError(f"Delimiter error. Expected {TCPMessage.delim:02x}, got {delim:02x}")

            if pos + cls.HEADER_SIZE > len(data):
                break

            unit_type: UnitType = int.from_bytes(data[pos+4:pos+8], byteorder='little')
            payload_size = int.from_bytes(data[pos+8:pos+12], byteorder='little')
            package_type: TcpPackageType = int.from_bytes(data[pos+12:pos+16], byteorder='little')
            src_uuid = int.from_bytes(data[pos+16:pos+24], byteorder='little')
            tgt_uuid = int.from_bytes(data[pos+24:pos+32], byteorder='little')
            host_unit_uuid = int.from_bytes(data[pos+32:pos+40], byteorder='little')

            end_payload = pos + cls.HEADER_SIZE + payload_size

            obj = cls.__new__(cls)
            if end_payload <= len(data):
                obj.__unit_type = unit_type
                obj.__package_type = package_type
                obj.__src_uuid = src_uuid
                obj.__tgt_uuid = tgt_uuid
                obj.__host_unit_uuid = host_unit_uuid
                obj.__payload = data[pos+cls.HEADER_SIZE:end_payload]
                obj.__complete = True
                yield obj
            else:
                obj.__complete = False
                yield obj
                break

            pos = end_payload

    def __init__(self, unit_type: UnitType = UnitType.UNDEFINED_UNIT_TYPE, package_type: TcpPackageType = TcpPackageType.UNDEFINED_PACKAGE_TYPE,
                 src_uuid: int = 0, tgt_uuid: int = 0, host_unit_uuid: int = 0) -> None:
        self.__unit_type: UnitType = unit_type
        self.__package_type: TcpPackageType = package_type
        self.__payload: bytearray = bytearray()
        self.__src_uuid: int = src_uuid
        self.__tgt_uuid: int = tgt_uuid
        self.__host_unit_uuid: int = host_unit_uuid
        self.__complete = False

    @property
    def unit_type(self) -> UnitType:
        return self.__unit_type

    @property
    def complete(self) -> bool:
        return self.__complete

    @property
    def payload_size(self) -> int:
        return len(self.__payload)

    @property
    def package_type(self) -> TcpPackageType:
        return self.__package_type

    @property
    def size(self) -> int:
        return len(self.__bytes__())

    @property
    def metadata_size(self) -> int:
        return self.size - self.payload_size

    @property
    def src_uuid(self) -> int:
        return self.__src_uuid

    @src_uuid.setter
    def src_uuid(self, val: int) -> None:
        self.__src_uuid = val
    
    @property
    def tgt_uuid(self) -> int:
        return self.__tgt_uuid

    @tgt_uuid.setter
    def tgt_uuid(self, val: int) -> None:
        self.__tgt_uuid = val

    @property
    def host_unit_uuid(self) -> int:
        return self.__host_unit_uuid

    @host_unit_uuid.setter
    def host_unit_uuid(self, val: int) -> None:
        self.__host_unit_uuid = val

    @property
    def payload(self) -> bytes:
        return bytes(self.__payload)

    @payload.setter
    def payload(self, data: bytearray | bytes | str) -> None:
        if isinstance(data, str):
            self.__payload = data.encode("utf-8")
        elif isinstance(data, bytearray):
            self.__payload = data
        elif isinstance(data, bytes):
            self.__payload = data
        else:
            raise ValueError("Data should be either a bytearray or a string but got {}".format(type(data)))

    def __repr__(self) -> str:
        return f"0x{TCPMessage.delim:02x}"

    def __str__(self) -> str:
        if self.package_type == TcpPackageType.TEXT:
            return (f"[0x{TCPMessage.delim:02x} | {self.unit_type} | {self.payload_size} | {self.package_type} | {self.src_uuid}  | {self.tgt_uuid} ]"
                    f" : Payload(ASCII): {str(self.payload.decode())}")
        else:
            return (f"[0x{TCPMessage.delim:02x} | {self.unit_type} | {self.payload_size} | {self.package_type} | {self.src_uuid}  | {self.tgt_uuid} ]"
                    " : Payload(ASCII): Something.")

    def __bytes__(self) -> bytes:
        delim_bytes: bytes = TCPMessage.delim.to_bytes(4, byteorder='little')
        unit_bytes: bytes = self.unit_type.to_bytes(4, byteorder='little')
        payload_size_bytes: bytes = self.payload_size.to_bytes(4, byteorder='little')
        package_type_bytes: bytes = self.package_type.to_bytes(4, byteorder='little')
        src_uuid_bytes: bytes = self.src_uuid.to_bytes(8, byteorder='little')
        tgt_uuid_bytes: bytes = self.tgt_uuid.to_bytes(8, byteorder='little')
        host_unit_uuid_bytes: bytes = self.host_unit_uuid.to_bytes(8, byteorder='little')
        payload_bytes: bytearray = self.payload
        return delim_bytes + unit_bytes + payload_size_bytes + package_type_bytes + src_uuid_bytes + tgt_uuid_bytes + host_unit_uuid_bytes + payload_bytes
