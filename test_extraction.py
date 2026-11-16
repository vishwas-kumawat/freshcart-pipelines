from datetime import date
import gzip
import json
import os
from pathlib import Path
from tempfile import TemporaryDirectory

from elt import extract


def main():
    run_id = "baseline_test_001"
    run_date = date(2026, 9, 30)
    session = extract.http_session()
    sources = {
        "web": extract.web_orders(session, run_date, mock=True),
        "mobile": extract.mobile_orders(session, run_date, mock=True),
        "crm": extract.crm_tier_events(session, run_date, mock=True),
        "fx": extract.fx_rates(
            session,
            date(2026, 8, 26),
            run_date,
            mock=True,
        ),
    }

    original_landing_dir = os.environ.get("LANDING_DIR")
    try:
        with TemporaryDirectory() as landing_dir:
            os.environ["LANDING_DIR"] = landing_dir
            for source, records in sources.items():
                path = extract.write_landing(
                    source,
                    records,
                    run_id,
                    run_date,
                )
                print(f"{source}: {len(records)} records | {path}")

                assert path is not None
                assert Path(path).exists()
                with gzip.open(path, "rt", encoding="utf-8") as file:
                    first_record = json.loads(file.readline())

                assert "source" in first_record
                assert "run_id" in first_record
                assert "extracted_at" in first_record
                assert "data" in first_record
    finally:
        session.close()
        if original_landing_dir is None:
            os.environ.pop("LANDING_DIR", None)
        else:
            os.environ["LANDING_DIR"] = original_landing_dir

    print("\nExtraction test completed successfully.")


if __name__ == "__main__":
    main()