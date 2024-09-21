#/usr/bin/pyrhon3
import argparse
import asyncio
from async_web_mirror import AsyncWebMirror

async def main():
    parser = argparse.ArgumentParser(description='ウェブサイトをミラーリングします。')
    parser.add_argument('url', help='ミラーリングするウェブサイトのURL')
    parser.add_argument('output_dir', help='ミラーリングしたファイルを保存するディレクトリ')
    parser.add_argument('-c', '--connections', type=int, default=50, help='同時接続数 (デフォルト: 50)')
    parser.add_argument('-w', '--weights', nargs='+', help='URLの優先度を設定する文字列（スペース区切りで複数指定可能）')
    parser.add_argument('-v', '--exclude', nargs='+', help='除外するURLのパターン（スペース区切りで複数指定可能）')
    args = parser.parse_args()

    mirror = AsyncWebMirror(args.url, args.output_dir, max_connections=args.connections, 
                            weights=args.weights, excluded_urls=args.exclude)
    await mirror.mirror_site()
    print("ミラーリングが完了しました。")

if __name__ == "__main__":
    asyncio.run(main())

