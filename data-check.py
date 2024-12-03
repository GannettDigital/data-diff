import argparse
from data_diff.hashdiff_tables import HashDiffer  # Add this import

def create_parser():
    parser = argparse.ArgumentParser()
    # ...existing code...
    
    parser.add_argument(
        "--chunk-size",
        type=int,
        help="Process table in fixed-size chunks (disables bisection). Value specifies rows per chunk.",
        default=None,
    )
    
    parser.add_argument(
        "--stop-at-top-level",
        action="store_true",
        help="Stop as soon as any difference is found at the top level (faster but less detailed)",
        default=False,
    )
    
    # ...existing code...

def main():
    # ...existing code...
    differ = HashDiffer(
        bisection_factor=args.bisection_factor,
        stop_at_top_level=args.stop_at_top_level,  # Add this line
        chunk_size=args.chunk_size,  # Add this line
        threaded=args.threads > 1,
        max_threadpool_size=args.threads,
    )
    # ...existing code...

if __name__ == "__main__":
    parser = create_parser()
    args = parser.parse_args()
    main()