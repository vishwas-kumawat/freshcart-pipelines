
from .extract import get_landing_dir
from .snowflake_io import (
    get_snowflake_connection,
    load_file,
)


SOURCES = {
    "web": "ORDERS_RAW",
    "mobile": "ORDERS_RAW",
    "crm": "CUSTOMERS_RAW",
    "fx": "FX_RAW",
    "pincode": "PINCODE_RAW",
}


def source_files(source: str):
    """Return landing files using the same configured root as extraction."""
    if source not in SOURCES:
        raise ValueError(f"Unsupported source: {source}")
    return sorted(
        (get_landing_dir() / source).glob("*/*.json.gz")
    )


def main():
    connection = get_snowflake_connection()

    try:
        total_files = 0

        for source, target in SOURCES.items():

            files = source_files(source)

            if not files:
                print(f"\nNo files found for {source}")
                continue

            for file_path in files:

                print(
                    f"\nSource: {source} | Target: {target}"
                )

                load_file(
                    connection,
                    file_path,
                    source
                )

                total_files += 1

        print(
            f"\nBronze loading completed. "
            f"Files processed: {total_files}"
        )

    finally:
        connection.close()


if __name__ == "__main__":
    main()