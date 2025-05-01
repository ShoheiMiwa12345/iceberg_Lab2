# AWS Glue 5.0 でのApache Iceberg 実践ガイド 
## AddValue: Qlik Answers Structured Data Using Case

## はじめに

Apache Icebergはオープンソースのテーブルフォーマットで、大規模なデータセットを効率的に管理するために設計されています。AWS Glue 5.0と組み合わせることで、堅牢なデータレイク運用基盤を構築できます。このガイドでは、AWS Glue 5.0を使ったIcebergテーブルの作成、データ操作、およびバージョン管理機能の活用について説明します。

## 環境セットアップ

このガイドでは、以下のコンポーネントで構成される環境を使用します：

- **AWS Glue 5.0**: ETLジョブ実行環境
- **Apache Iceberg**: テーブルフォーマット
- **Amazon S3**: データストレージ
- **AWS Glue Data Catalog**: Icebergテーブルのメタデータカタログ
- **JupyterLab**: 対話的な開発・テスト環境

## 前提条件

- Dockerがインストールされていること
- AWS 認証情報が設定されており、Glue.DynamoDB.S3の作成変更削除ロールが付与されている事
- 基本的なPython、Sparkの知識

## 追加コンテンツ

Apache Iceberg 実践ガイド完了後、Qlik Cloud Analytics を想定した構造化データの活用を行うシナリオを設けています。追加コンテンツでは、データフロー/Qlik Load Script および Qlik Answers を使用し、非構造化データと構造化データを統合RAG を作成します。

- Qlik Cloud Analytics (Qlik Answers) が利用可能であること
- Qlik Cloud Analytics へ本編で使用するS3接続の作成が可能であること


## 環境構築

AWS Glue 5.0をベースとしたDockerイメージを使用して、ローカル開発環境をセットアップします。

### Dockerイメージの作成

Dockerfile.livy_jupyterファイルを使用して環境を構築します。このファイルには、AWS Glue 5.0イメージをベースに、Livy、JupyterLab、必要なライブラリなどをインストールする手順が含まれています。

### 環境の起動

以下のコマンドでDockerイメージをビルドし、実行します：

```bash
# Dockerイメージをビルド
docker build -t glue-livy-jupyter -f Dockerfile.livy_jupyter .

# 作業ディレクトリを設定
export WORKSPACE_LOCATION=$(pwd)/notebooks

# Dockerコンテナを実行
docker run -it --rm \
  -v ~/.aws:/home/hadoop/.aws \
  -v $WORKSPACE_LOCATION:/home/hadoop/workspace/ \
  -p 8998:8998 \
  -p 8888:8888 \
  --name glue5_jupyter \
  glue-livy-jupyter
```

コンテナが起動したら、ブラウザで以下のURLにアクセスしてJupyterLabを開きます：

```
http://localhost:8888
```

# JupyterLab 上の注意点

コードは記述済となっており、Ctrl + Enter により実行可能な状態ですが、以下の情報は AWS Configure および S3バケット名は実際にご利用のものを入力してください。

os.environ['AWS_ACCESS_KEY_ID'] = 'XXXXXXXXXXXXXXX'
os.environ['AWS_SECRET_ACCESS_KEY'] = 'XXXXXXXXXXXXXXX'
os.environ['AWS_DEFAULT_REGION'] = 'XXXXXXXXXXXXXXX'

s3://xxxxxxxxxxxxxxxxx

## Iceberg テーブルの基本操作

JupyterLabで新しいノートブックを作成し、以下の手順でApache Icebergを使ったデータ処理を行います。

### 必要なパッケージとSparkセッションの設定

AWS Glue環境でApache Icebergを使用するために、必要なパッケージをインストールし、適切な設定でSparkセッションを初期化します。Sparkセッションの設定では、Icebergのカタログ実装やS3ストレージの設定などを指定します。

### Icebergテーブルの作成

Icebergテーブルを作成する場合は、通常のSQLテーブル作成構文に `USING iceberg` を追加します。また、パーティショニングを行う場合は `PARTITIONED BY` 句で指定します。

例：顧客フィードバックテーブルの作成
```sql
CREATE TABLE glue_catalog.customer_feedback_db.customer_feedback (
    feedback_id STRING,
    customer_id STRING,
    feedback_date DATE,
    rating INT,
    feedback_text STRING,
    product_id STRING
) USING iceberg
PARTITIONED BY (days(feedback_date))
```

日付型カラムで日単位のパーティショニングを行う場合は、上記のように `days()` 関数を使用します。

## データ操作（CRUD処理）

Icebergテーブルでは、標準的なSQL構文を使用してデータの挿入、取得、更新、削除を行えます。

### データの挿入

```sql
INSERT INTO glue_catalog.customer_feedback_db.feedback_reports VALUES
('REP202504', CAST('2025-04-30' AS DATE), CAST('2025-04-01' AS DATE), CAST('2025-04-30' AS DATE), 
 247, 4.2, 68, '顧客サポート部門', 's3://xxxxxxxxxxxxxxxxx/reports/feedback_report_202504.pdf')
```

### データの取得

特定の条件でデータを取得する例：
```sql
SELECT c.customer_name, c.feedback_date, c.rating, c.feedback_text
FROM glue_catalog.customer_feedback_db.customer_feedback c
WHERE c.product_id = 'PROD-A'
ORDER BY c.feedback_date DESC
```

### データの更新

```sql
UPDATE glue_catalog.customer_feedback_db.feedback_reports
SET total_feedback_count = 249, average_satisfaction = 4.1
WHERE report_id = 'REP202504'
```

### データの削除

```sql
DELETE FROM glue_catalog.customer_feedback_db.customer_feedback
WHERE feedback_id = 'FB005'
```

## Icebergのスナップショットとバージョン管理

Icebergは、各テーブル操作でスナップショットを作成し、データの変更履歴を追跡します。これにより、任意の時点のテーブル状態にアクセスできます。

### テーブル履歴の確認

テーブルの変更履歴を確認するには、特殊なメタデータテーブルを参照します：

```sql
SELECT * FROM glue_catalog.customer_feedback_db.customer_feedback.history
```

### スナップショット情報の表示

特定のテーブルのスナップショット情報を表示します：

```sql
SELECT * FROM glue_catalog.customer_feedback_db.customer_feedback.snapshots
```

### 特定時点のデータにアクセス

Icebergでは、特定のスナップショットIDを使用して過去の任意の時点のデータにアクセスできます：

```sql
SELECT * FROM glue_catalog.customer_feedback_db.customer_feedback VERSION AS OF 123456789
```

上記の例では、スナップショットID `123456789` の時点のデータを取得します。

### 時間旅行（Time Travel）機能の活用

スナップショット間の変更を比較したり、過去のデータバージョンを復元したりできます。これにより、誤ったデータ更新や削除からの復旧が可能になります。

## パーティショニングと最適化

Icebergはパーティショニングとテーブル最適化機能を提供します。

### パーティション情報の確認

テーブルのパーティション情報を確認するには：

```sql
SELECT * FROM glue_catalog.customer_feedback_db.customer_feedback.partitions
```

特定のパーティションのみを効率的に取得する例：

```sql
SELECT * FROM glue_catalog.customer_feedback_db.customer_feedback 
WHERE feedback_date = CAST('2025-04-15' AS DATE)
```

### ファイル最適化とコンパクション

Icebergでは、小さなファイルを統合してパフォーマンスを向上させるためのコンパクション操作を提供しています：

```sql
CALL glue_catalog.system.rewrite_data_files(table => 'customer_feedback_db.customer_feedback')
```

### テーブルプロパティの設定

テーブルの動作を制御するプロパティを設定します：

```sql
ALTER TABLE glue_catalog.customer_feedback_db.customer_feedback 
SET TBLPROPERTIES (
  'comment' = 'Contains customer feedback data partitioned by date',
  'write.format.default' = 'parquet',
  'write.parquet.compression-codec' = 'snappy'
)
```

## スキーマ進化

Icebergはスキーマ進化をサポートし、既存データに影響を与えずにスキーマを変更できます。

Icebergでは、以下のようなスキーマ変更操作がサポートされています：

- 新しいカラムの追加
- カラム名の変更
- カラムのデータ型変更（互換性のある変更のみ）
- 必須カラムの任意カラムへの変更
- カラムの削除またはドロップ
- カラムのコメント変更

これらの変更は既存データに影響を与えず、新しいスキーマが適用された後も古いスキーマでデータを読み取ることができます。

## テーブルメンテナンス

Icebergテーブルでは、定期的なメンテナンス作業が推奨されます。

### 期限切れスナップショットの管理

古いスナップショットを期限切れにして、ストレージスペースを節約します：

```sql
ALTER TABLE glue_catalog.customer_feedback_db.customer_feedback
SET TBLPROPERTIES (
    'history.expire.min-snapshots-to-keep' = '2',
    'history.expire.max-snapshot-age-ms' = '259200000'
)

CALL glue_catalog.system.expire_snapshots(table => 'customer_feedback_db.customer_feedback')
```

### データファイルの最適化

パフォーマンスを向上させるためにデータファイルを再構成します：

```sql
CALL glue_catalog.system.rewrite_data_files(table => 'customer_feedback_db.customer_feedback', options => map('min-input-files','2'))
```

## 高度な分析クエリ

Icebergテーブルでは、標準的なSQL分析クエリを実行できます。

### 製品別の評価分析

```sql
SELECT 
    cf.product_id,
    COUNT(*) as feedback_count,
    AVG(cf.rating) as avg_rating,
    COUNT(CASE WHEN cf.rating >= 4 THEN 1 END) as positive_count,
    COUNT(CASE WHEN cf.rating <= 2 THEN 1 END) as negative_count
FROM glue_catalog.customer_feedback_db.customer_feedback cf
GROUP BY cf.product_id
ORDER BY avg_rating DESC
```

### 時系列分析

```sql
SELECT 
    feedback_date,
    COUNT(*) as count,
    AVG(rating) as avg_rating
FROM glue_catalog.customer_feedback_db.customer_feedback
GROUP BY feedback_date
ORDER BY feedback_date
```

### カテゴリごとのポジティブ・ネガティブ比率の推移
フィードバックカテゴリ別の評価傾向を分析します：

# 追加コンテンツ

### データのエクスポートと統合
Icebergテーブルからデータをエクスポートして、他のシステムと統合することができます。

### 特定バージョンのデータエクスポート
スナップショットIDを指定して特定時点のデータをエクスポートします

# Qlik Cloud 上から必要なリソースの作成
"データ接続の作成" から Iceberg テーブルからエクスポートしたデータ先となる S3バケットの接続を作成します。

![image](https://github.com/user-attachments/assets/4968e2aa-21e7-418f-89a2-149a7854b6c9)

リポジトリ内のScript(**Script.qvf**) をインポート、またはデータフローを開き、txt 変換を実施します。

![image](https://github.com/user-attachments/assets/8f11db91-3e07-4092-81f2-1277e36fd09c)

![image](https://github.com/user-attachments/assets/3916d36a-98bd-4492-a92f-62c93cdd30b4)

# Qlik Answers KB(Knowleage Base) を作成し、PDF と txt のインデキシング
リポジトリ内にある ***customer-feedback-report.pdf*** をKBへアップロードします。
先ほどテキスト出力したバケットをセットします。
インデックス化を実施します。


![image](https://github.com/user-attachments/assets/7434907c-d90c-4602-9d23-cdb10694c1a7)

# Qlik Answers Structured Data Using Case
アシスタントを作成し、質問を投げて、非構造化データと構造化データを統合的に解釈した回答を得ます。


![image](https://github.com/user-attachments/assets/b38212b1-2fb9-4c69-97e7-bf446a3640ad)


![image](https://github.com/user-attachments/assets/a6ec9505-11c2-45d6-b190-b57323aca3c8)

![image](https://github.com/user-attachments/assets/d6b7db52-a021-4188-a9d1-90b4c4c0745b)



## まとめ

このガイドでは、AWS Glue 5.0環境でApache Icebergを使用する方法について説明しました。Icebergのメリットは以下の通りです：

1. スナップショットによるバージョン管理と時間旅行機能
2. 効率的なパーティショニングとデータ管理
3. スキーマ進化のサポート
4. データファイルの最適化機能
5. 標準SQLを使用した操作性

これらの機能により、Icebergは大規模データセットの管理に適したテーブルフォーマットとなっています。

## 参考資料

- [Apache Iceberg 公式ドキュメント](https://iceberg.apache.org/)
- [Docker コンテナを使って AWS Glue 5.0 のジョブをローカルで開発・テストする](https://aws.amazon.com/jp/blogs/news/develop-and-test-aws-glue-5-0-jobs-locally-using-a-docker-container/)
- [AWS Glue for Apache Iceberg](https://docs.aws.amazon.com/glue/latest/dg/aws-glue-programming-etl-format-iceberg.html)
