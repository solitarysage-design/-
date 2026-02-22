# J-Quants v2 Light 増配バリュー株スクリーナー

## セットアップ
```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

## Secrets
- `JQUANTS_API_KEY`: J-Quants API v2 の APIキー
- `SLACK_WEBHOOK_URL`: Slack Incoming Webhook URL

GitHub Actions では上記を Repository Secrets に設定してください。

## 実行
```bash
python main.py --dry-run
python main.py --no-slack
python main.py --max-codes 300
```

- `--dry-run`: APIキー不要で構文と出力導線を確認（`output/screen_results.csv` を生成）
- `--no-slack`: Slack投稿を抑制
- `--max-codes`: ローカル検証用に銘柄数を制限

## 実装概要
- Slack通知は Block Kit（Header / Context / 俯瞰 / 銘柄カード）で見やすく表示（TopNのみ投稿、CSVは全件保存）
- Bulk API (`/bulk/list` -> `/bulk/get`) で以下を取得
  - `/equities/master`
  - `/equities/bars/daily`
  - `/fins/summary`
- 一次通過銘柄のみ `/fins/details` を個別取得し、NetDebt/EBITDA と Interest Coverage を補完
- 列名差異にはファジー検出で対応し、推定結果をログ出力
- `strict_mode` で details 指標が計算不能な場合の扱いを切り替え
- Slack失敗時もCSV保存は継続

## 出力
- `output/screen_results.csv`
- ログで件数を表示: 取得件数 -> Hard+価値条件通過件数 -> TopN

## トラブルシュート
- Bulk失敗: APIキー・レート制限・一時エラーを確認（実装は指数バックオフ付きリトライ）
- 列名不一致: ログの `details mapping` で検出列を確認。必要なら `main.py` の候補列を追加
- Slack失敗: webhook URL と権限を確認。失敗しても CSV は残る仕様


## GitHub Actions スケジュール
- 毎週 月曜 07:05 JST に実行（UTC cron: `5 22 * * 0`）
- `workflow_dispatch` で手動実行可能
- `output/screen_results.csv` をartifact保存
