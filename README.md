# Redshift Large Data Volume Example Connector

## 概要

このリポジトリは、Fivetran 公式の `redshift/large_data_volume` テンプレートをベースに、Amazon Redshift の大容量テーブルを Connector SDK 経由で同期するための検証・拡張実装です。

主な拡張点は以下です。

- テーブル単位の JSON 設定
- Redshift メタデータからの PK 自動検出
- timestamp/date 型からの `replication_key` 自動推定
- VARCHAR / 数値列を明示的な `replication_key` として使用可能
- 大容量テーブル向けチャンク処理
- PK あり増分同期での bookmark 境界再取得
- 全件置換データ向け `SNAPSHOT` 戦略
- 複数テーブルの並列処理
- GitHub Actions による自動テスト

---

## 動作要件

- [Fivetran Connector SDK がサポートする Python バージョン](https://github.com/fivetran/community_connectors/blob/main/README.md#requirements)
- Windows 10 以降（64bit）
- macOS 13 Ventura 以降
- Ubuntu 20.04 / Debian 10 / Amazon Linux 2 以降など

---

## セットアップ

Connector SDK の初期セットアップは [Connector SDK Setup Guide](https://fivetran.com/docs/connectors/connector-sdk/setup-guide) を参照してください。

公式テンプレートを初期化する場合:

```bash
fivetran init --template redshift/large_data_volume
```

`fivetran debug` を実行する前に `configuration.json` の接続情報を設定してください。

---

## configuration.json

```json
{
  "redshift_host": "<YOUR_REDSHIFT_HOST>",
  "redshift_port": "<YOUR_REDSHIFT_PORT>",
  "redshift_database": "<YOUR_REDSHIFT_DATABASE>",
  "redshift_user": "<YOUR_REDSHIFT_USER>",
  "redshift_password": "<YOUR_REDSHIFT_PASSWORD>",
  "redshift_schema": "<YOUR_REDSHIFT_SCHEMA>",
  "batch_size": "<YOUR_BATCH_SIZE_FOR_FETCHING_DATA>",
  "auto_schema_detection": "<ENABLE_OR_DISABLE_AUTO_SCHEMA_DETECTION>",
  "enable_complete_resync": "<ENABLE_OR_DISABLE_FULL_RESYNC_DURING_EACH_SYNC>",
  "max_parallel_workers": "<NUMBER_OF_PARALLEL_WORKERS>"
}
```

### 主な設定項目

- `batch_size`
  - 1 回の FETCH で取得する行数
  - `use_chunking=true` の場合はチャンク目標件数としても使用
- `auto_schema_detection`
  - Redshift 内のテーブルを自動検出する場合は `true`
  - このフォークの `tables/*.json` を使う場合は基本的に `false`
- `enable_complete_resync`
  - 全テーブルを毎回 FULL として扱うためのグローバル設定
- `max_parallel_workers`
  - 並列同期する最大テーブル数
  - 目安は 2～4 程度

> 実運用用の `configuration.json` には認証情報が含まれるため、公開リポジトリへコミットしないでください。

---

## requirements.txt

```text
redshift_connector
```

---

# テーブル設定

テーブルごとの設定は Python コードではなく `tables/` 配下の JSON ファイルで管理します。

```text
tables/
├── _defaults.json
├── sales.orders.json
├── sales.customers.json
└── examples/
    ├── example.pk_timestamp.json
    ├── example.pk_no_timestamp.json
    ├── example.no_pk_timestamp.json
    └── example.no_pk_no_timestamp.json
```

有効な設定ファイルは `tables/` 直下のみです。`tables/examples/` などのサブディレクトリは読み込まれません。

ファイル名がテーブル識別子になります。

```text
<schema>.<table>.json
```

例:

```text
tables/public.orders.json
```

`tables/_defaults.json` が共通デフォルトで、各テーブル JSON は差分だけを記述できます。

```json
{
  "replication_key": "updated_at",
  "use_chunking": true
}
```

空 JSON も有効です。

```json
{}
```

---

## primary_keys

`null` の場合は Redshift メタデータから PK を自動検出します。

```json
{ "primary_keys": null }
```

PK が存在しないことを明示する場合:

```json
{ "primary_keys": [] }
```

複合キーを手動指定する場合:

```json
{ "primary_keys": ["order_id", "line_no"] }
```

---

## strategy

利用可能な値:

```text
AUTO
FULL
INCREMENTAL
SNAPSHOT
```

### AUTO

timestamp/date 型の `replication_key` を取得できる場合は INCREMENTAL、取得できない場合は FULL へフォールバックします。

### INCREMENTAL

bookmark を利用して変更分だけを取得します。`replication_key` を明示した場合は timestamp 型でなくても利用できます。

```json
{
  "strategy": "INCREMENTAL",
  "replication_key": "change_seq"
}
```

### FULL

毎回ソース全件を読み取り、`op.upsert()` で送信します。宛先テーブルの事前 truncate は行いません。

### SNAPSHOT

ソーステーブルが毎回「現在の全件イメージ」として生成される場合に使用します。

```text
op.truncate()
    ↓
Redshift 全件取得
    ↓
connector_snapshot_row_id を各行へ付与
    ↓
op.upsert()
```

---

## replication_key

`null` の場合は timestamp/date 型の列から自動推定します。

```json
{ "replication_key": null }
```

timestamp 型ではない列を変更キーとして使う場合は明示指定します。

VARCHAR 例:

```json
{ "replication_key": "update_time" }
```

数値例:

```json
{ "replication_key": "change_seq" }
```

`change_seq` は PK である必要はなく、値の重複も許容されます。ただし変更時に値が前進し、bookmark より過去へ戻らないことが前提です。

---

## enabled

一時的に同期対象から外す場合:

```json
{ "enabled": false }
```

---

# PK あり増分同期の bookmark 境界

PK を持つ INCREMENTAL テーブルでは、保存済み bookmark と同じ `replication_key` 値を次回同期時に再取得します。

```sql
WHERE replication_key >= :bookmark
```

同一 timestamp / sequence を持つ複数行が境界に存在しても取りこぼさないためです。

再取得された行は PK ベースの `op.upsert()` により同一行として処理されます。

チャンク処理では、最初のチャンクだけ保存済み bookmark を `>=` で再取得し、同一実行内の次チャンク以降は `>` を使用して前進します。

---

# batch_size とチャンクサイズ

`use_chunking=true` の場合、`configuration.json` の `batch_size` をチャンク目標件数としても使用します。固定の `CHUNK_SIZE` はありません。

例:

```text
batch_size = 20000

FETCH 単位       = 最大 20,000 行
チャンク目標件数 = 約 20,000 行
```

チャンク境界は `replication_key` でソートした結果のおおよそ `batch_size` 番目の値で決定します。

同じ `replication_key` 値を持つ行は同一チャンクに含めるため、実際の件数が `batch_size` を超える場合があります。

---

# SNAPSHOT 戦略

SNAPSHOT は、PK がなく、ソース側で毎回全件置換されるテーブルを想定しています。

```json
{
  "primary_keys": [],
  "strategy": "SNAPSHOT",
  "replication_key": null,
  "use_chunking": true
}
```

timestamp/date 型の列が存在する場合、その列をチャンク境界として自動推定できます。

ただし SNAPSHOT は過去 sync の bookmark を次回開始位置として使用せず、毎回最初から全件を再取得します。

## SNAPSHOT の synthetic primary key

Connector SDK には INSERT 専用 operation がないため、SNAPSHOT では各行に以下の synthetic key を追加します。

```text
connector_snapshot_row_id
```

例:

```text
ソース
A | 100
A | 100
A | 100

送信時
1 | A | 100
2 | A | 100
3 | A | 100
```

これにより完全に同じ内容の行でも別行として保持できます。

synthetic key はテーブル全体で 1, 2, 3... と連番になり、チャンクをまたいでもリセットされません。

---

# 想定テーブルパターン

| パターン | PK | timestamp | 処理 |
| --- | --- | --- | --- |
| 1. 有有 | あり | あり | INCREMENTAL |
| 2. 有無 | あり | なし | 代替変更キーがあれば INCREMENTAL |
| 3. 無有 | なし | あり | SNAPSHOT（全件置換データ） |
| 4. 無無 | なし | なし | 対象外 / 未対応 |

## パターン 1: PK あり / timestamp あり

基本的には自動設定できます。

```json
{}
```

必要に応じて timestamp を明示します。

```json
{
  "replication_key": "updated_at",
  "use_chunking": true
}
```

## パターン 2: PK あり / timestamp なし

timestamp の代わりとなる変更キーを明示します。

```json
{
  "replication_key": "update_time",
  "use_chunking": true
}
```

または:

```json
{
  "replication_key": "change_seq",
  "use_chunking": true
}
```

単純な連番、FK、複合 PK の一部である detail sequence など、更新時刻・変更順序を表さない値は `replication_key` として使用できません。

## パターン 3: PK なし / timestamp あり

対象テーブルが毎回全件置換される前提で SNAPSHOT を使用します。

```json
{
  "primary_keys": [],
  "strategy": "SNAPSHOT",
  "replication_key": null,
  "use_chunking": true
}
```

## パターン 4: PK なし / timestamp なし

このフォークでは未対応です。安全な増分位置も行識別子も存在しないため、自動同期方法は提供しません。

---

# テスト

GitHub Actions では主に以下を検証します。

- Python 3.9 / 3.11
- table spec の読み込み
- `primary_keys=null` / `[]` の区別
- INCREMENTAL bookmark の `>=` / `>` 境界
- 数値 `replication_key` の bookmark 型保持
- `batch_size` とチャンクサイズの連動
- SNAPSHOT strategy の validation
- SNAPSHOT synthetic PK
- 完全重複行への異なる synthetic key 付与
- SNAPSHOT 開始時の `op.truncate()`
- SNAPSHOT が以前の bookmark を再利用しないこと
- チャンク間で synthetic row id が連続すること

---

## 注意事項

このリポジトリは Fivetran 公式 Redshift Large Data Volume サンプルをベースにした検証・拡張実装です。

実環境へ適用する前に、Redshift / Fivetran / Snowflake 環境で以下を確認してください。

- 初回同期
- 2 回目以降の増分同期
- bookmark 境界
- 同一 timestamp / sequence の大量重複
- 中断後の再実行
- チャンク処理
- 列追加 / 削除
- SNAPSHOT の truncate / reload
- 完全重複行の保持
- 大容量テーブルでの処理時間・負荷
