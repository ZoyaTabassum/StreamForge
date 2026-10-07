import json
import os
from typing import Any, Optional

from rocksdict import Rdict


class StateStore:
    """
    Persistent RocksDB state store for StreamForge.
    """

    def __init__(self, path: Optional[str] = None):
        if path is None:
            path = os.getenv(
                "ROCKSDB_PATH",
                os.path.join(
                    os.path.dirname(__file__),
                    "data",
                    "streamforge_state"
                ),
            )

        self.path = path
        os.makedirs(os.path.dirname(self.path), exist_ok=True)

        self.db = Rdict(self.path)

    def put(self, key: str, value: dict[str, Any]) -> None:
        self.db[key] = json.dumps(value)

    def get(self, key: str) -> Optional[dict[str, Any]]:
        value = self.db.get(key)

        if value is None:
            return None

        if isinstance(value, bytes):
            value = value.decode("utf-8")

        if isinstance(value, str):
            return json.loads(value)

        return value

    def delete(self, key: str) -> None:
        if key in self.db:
            del self.db[key]

    def exists(self, key: str) -> bool:
        return key in self.db

    def keys(self):
        return list(self.db.keys())

    def count(self) -> int:
        return len(self.db)

    def flush(self) -> None:
        self.db.flush()

    def close(self) -> None:
        self.db.flush()
        self.db.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.close()


if __name__ == "__main__":
    print("Testing StreamForge RocksDB state store...")

    store = StateStore()

    test_state = {
        "total_temperature": 125.5,
        "message_count": 4,
        "average_temperature": 31.375,
        "window_id": 80523,
    }

    store.put("TRUCK-TEST", test_state)

    recovered = store.get("TRUCK-TEST")

    print("Stored:")
    print(test_state)

    print("\nRecovered:")
    print(recovered)

    if recovered == test_state:
        print("\nRocksDB persistence test PASSED")
    else:
        print("\nRocksDB persistence test FAILED")

    store.close()