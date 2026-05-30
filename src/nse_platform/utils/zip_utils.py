import io
import zipfile


def unzip_single_csv(zip_bytes: bytes) -> bytes:
    """Extract the single CSV from a ZIP archive given as bytes.

    Returns the CSV content as bytes. Raises ValueError if the archive
    does not contain exactly one CSV.
    """
    # 1. wrap the bytes so zipfile can treat them as a file
    buffer = io.BytesIO(zip_bytes)

    # 2. open the buffer as a zip archive
    with zipfile.ZipFile(buffer) as zf:

        # 3. find the CSV name among the archive's members
        names = zf.namelist()
        csv_names = [n for n in names if n.lower().endswith(".csv")]
        if len(csv_names) == 0:
            raise ValueError(f"No CSV found in archive. Members: {names}")
        if len(csv_names) > 1:
            raise ValueError(
                f"Expected exactly one CSV, found {len(csv_names)}: {csv_names}")

        # 4. read and return that member's bytes
        csv_name = csv_names[0]
        return zf.read(csv_name)
