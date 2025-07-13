#!/usr/bin/python3
import argparse
import asyncio
import os
from .async_web_mirror import AsyncWebMirror

async def main_async():
    parser = argparse.ArgumentParser(description='Mirrors a website.')
    parser.add_argument('url', help='URL of the website to mirror')
    parser.add_argument('output_dir', help='Directory to save the mirrored files')
    parser.add_argument('-c', '--connections', type=int, default=50, help='Number of simultaneous connections (default: 50)')
    parser.add_argument('-w', '--weights', nargs='+', help='Strings to set URL priorities (can specify multiple separated by spaces)')
    parser.add_argument('-v', '--exclude', nargs='+', help='URL patterns to exclude (can specify multiple separated by spaces)')
    parser.add_argument('-f', '--force', action='store_true', help='Force re-download even if the download was already completed')
    args = parser.parse_args()

    os.makedirs(args.output_dir, exist_ok=True)
    
    if args.force and os.path.exists(os.path.join(args.output_dir, "COMPLETE")):
        os.remove(os.path.join(args.output_dir, "COMPLETE"))
        print("Forced download triggered: completion flag has been removed.")

    mirror = AsyncWebMirror(args.url, args.output_dir, max_connections=args.connections, 
                            weights=args.weights, excluded_urls=args.exclude)
    
    try:
        await mirror.mirror_site()
        print("Mirroring completed.")
    except Exception as e:
        print(f"\nAn error occurred: {e}")
        print("Progress has been saved and can be resumed later.")

def main():
    asyncio.run(main_async())

if __name__ == "__main__":
    main()
