from datetime import date

from nse_platform.config.settings import settings
from nse_platform.sources.nse_client import NseClient


def main():
    client = NseClient(
        base_url=settings.NSE_BASE_URL,
        udiff_cutover=settings.UDIFF_CUTOVER_DATE,
    )

    # 1. URL building — both formats, no network
    print("--- URL building ---")
    print("old format:", client.build_url(date(2024, 1, 2)))
    print("UDiFF format:", client.build_url(date(2024, 8, 14)))

    # 2. Real fetch — old format
    print("\n--- Old-format fetch ---")
    old_bytes = client.fetch_bhavcopy(date(2024, 1, 2))
    print(
        f"got {len(old_bytes)} bytes, starts with PK: {old_bytes[:2] == b'PK'}")

    # 3. Real fetch — UDiFF format
    print("\n--- UDiFF fetch ---")
    udiff_bytes = client.fetch_bhavcopy(date(2024, 8, 14))
    print(
        f"got {len(udiff_bytes)} bytes, starts with PK: {udiff_bytes[:2] == b'PK'}")


if __name__ == "__main__":
    main()
