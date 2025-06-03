from __future__ import print_function
import os.path
import base64
from email import message_from_bytes
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.api_core import exceptions
import pickle
import email
import time
import json
from datetime import datetime
from bs4 import BeautifulSoup

# スコープ設定（読み取りのみ）
SCOPES = ['https://www.googleapis.com/auth/gmail.readonly']

# BigQuery設定
PROJECT_ID = 'sho-naga'
DATASET_ID = 'ses_email'
TABLE_ID = 'testtest_email_messages'

def create_bq_table():
    """BigQueryのテーブルを作成する関数"""
    client = bigquery.Client()
    
    # データセットの参照を作成
    dataset_ref = f"{PROJECT_ID}.{DATASET_ID}"
    
    try:
        # データセットの存在確認
        client.get_dataset(dataset_ref)
    except exceptions.NotFound:
        print(f"データセット {DATASET_ID} が存在しないため、作成します。")
        # データセットが存在しない場合は作成
        dataset = bigquery.Dataset(dataset_ref)
        dataset.location = "US"
        client.create_dataset(dataset, exists_ok=True)
        print(f"データセット {DATASET_ID} を作成しました。")
    
    # テーブルの参照を作成
    table_ref = f"{dataset_ref}.{TABLE_ID}"
    
    # スキーマの定義
    schema = [
        bigquery.SchemaField("message_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("subject", "STRING"),
        bigquery.SchemaField("from_address", "STRING"),
        bigquery.SchemaField("date", "STRING"),
        bigquery.SchemaField("snippet", "STRING"),
        bigquery.SchemaField("body_preview", "STRING"),
        bigquery.SchemaField("processed_at", "TIMESTAMP")
    ]
    
    try:
        # テーブルの存在確認
        client.get_table(table_ref)
        print(f"テーブル {TABLE_ID} は既に存在します。")
    except exceptions.NotFound:
        print(f"テーブル {TABLE_ID} が存在しないため、作成します。")
        # テーブルが存在しない場合は作成
        table = bigquery.Table(table_ref, schema=schema)
        client.create_table(table)
        print(f"テーブル {TABLE_ID} を作成しました。")
    
    return table_ref

def insert_to_bigquery(email_data):
    """BigQueryにデータを挿入する関数"""
    client = bigquery.Client()
    table_ref = create_bq_table()
    
    # データの整形
    rows_to_insert = []
    for email in email_data:
        row = {
            "message_id": email["id"],
            "subject": email["subject"],
            "from_address": email["from"],
            "date": email["date"],
            "snippet": email["snippet"],
            "body_preview": email["body_preview"],
            "processed_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        rows_to_insert.append(row)
    
    # データの挿入
    errors = client.insert_rows_json(table_ref, rows_to_insert)
    if errors:
        print(f"データ挿入中にエラーが発生しました: {errors}")
        return False
    else:
        print(f"{len(rows_to_insert)}件のデータをBigQueryに正常に挿入しました。")
        return True

def get_authorization_url(flow):
    """認証URLを取得し、認証コードの入力を促す"""
    auth_url, _ = flow.authorization_url(prompt='consent', access_type='offline')
    print('以下のURLをブラウザで開いて認証を行ってください:')
    print(auth_url)
    print('\n認証後に表示されるコードを入力してください:')
    code = input()
    return code

def get_message_body(payload):
    """メール本文を取得する関数（全文取得）"""
    if not payload:
        return ""

    # 本文を格納する変数
    body_parts = []

    def decode_body(data):
        """Base64エンコードされた本文をデコードする"""
        try:
            return base64.urlsafe_b64decode(data).decode('utf-8')
        except UnicodeDecodeError:
            try:
                return base64.urlsafe_b64decode(data).decode('iso-2022-jp')
            except:
                try:
                    return base64.urlsafe_b64decode(data).decode('shift_jis')
                except:
                    return "[エンコーディングエラー: 本文を取得できませんでした]"

    def extract_body_from_part(part):
        """再帰的に本文を抽出する"""
        if 'parts' in part:
            # マルチパートメッセージの処理
            for p in part['parts']:
                extract_body_from_part(p)
        elif 'body' in part:
            mime_type = part.get('mimeType', '')
            if mime_type == 'text/plain':
                # プレーンテキストの処理
                if 'data' in part['body']:
                    body_parts.append(decode_body(part['body']['data']))
            elif mime_type == 'text/html':
                # HTML形式の処理（プレーンテキストが無い場合のバックアップ）
                if 'data' in part['body'] and not body_parts:
                    html_content = decode_body(part['body']['data'])
                    soup = BeautifulSoup(html_content, 'html.parser')
                    body_parts.append(soup.get_text())

    # ペイロードの処理を開始
    if 'parts' in payload:
        # マルチパートメッセージの処理
        for part in payload['parts']:
            extract_body_from_part(part)
    elif 'body' in payload:
        # シンプルなメッセージの処理
        if 'data' in payload['body']:
            body_parts.append(decode_body(payload['body']['data']))

    # 取得した本文をすべて結合
    full_body = '\n'.join(filter(None, body_parts))
    
    return full_body if full_body else "[本文なし]"

def get_all_messages(service, user_id='me', max_results=1000):
    """指定された件数のメールを取得する関数"""
    try:
        messages = []
        next_page_token = None
        
        while len(messages) < max_results:
            # 1回のリクエストで取得する件数を制限（APIの制限を考慮）
            remaining = min(max_results - len(messages), 100)
            response = service.users().messages().list(
                userId=user_id,
                maxResults=remaining,
                pageToken=next_page_token
            ).execute()

            if 'messages' in response:
                messages.extend(response['messages'])
                print(f"\r現在 {len(messages)}/{max_results} 件のメールを取得中...", end='')
                
                if len(messages) >= max_results:
                    break
                
                next_page_token = response.get('nextPageToken')
                if not next_page_token:
                    break
            else:
                break

        print(f"\n合計 {len(messages)} 件のメールを取得しました。")
        return messages[:max_results]  # 確実に制限数以内に収める
    except Exception as e:
        print(f'メール一覧の取得中にエラーが発生しました: {str(e)}')
        return []

def main():
    creds = None

    if os.path.exists('token.pickle'):
        try:
            with open('token.pickle', 'rb') as token:
                creds = pickle.load(token)
        except:
            os.remove('token.pickle')
            creds = None

    if not creds or not isinstance(creds, Credentials):
        flow = InstalledAppFlow.from_client_secrets_file(
            'this_is_secret.json',  
            SCOPES,
            redirect_uri='urn:ietf:wg:oauth:2.0:oob'
        )
        auth_code = get_authorization_url(flow)
        flow.fetch_token(code=auth_code)
        creds = flow.credentials

        with open('token.pickle', 'wb') as token:
            pickle.dump(creds, token)

    try:
        service = build('gmail', 'v1', credentials=creds)
        print("メールの取得を開始します...")
        messages = get_all_messages(service, max_results=1000)  # 1000件に制限

        if not messages:
            print('メッセージが見つかりませんでした。')
            return

        email_data = []
        batch_size = 50  # BigQueryへの一括挿入サイズ
        current_batch = []
        
        print("\nメールの詳細情報を取得しています...")
        for i, msg in enumerate(messages, 1):
            try:
                msg_detail = service.users().messages().get(
                    userId='me', 
                    id=msg['id'], 
                    format='full'
                ).execute()
                
                headers = msg_detail['payload']['headers']
                email_info = {
                    'id': msg['id'],
                    'subject': next(
                        (header['value'] for header in headers if header['name'].lower() == 'subject'),
                        'No Subject'
                    ),
                    'from': next(
                        (header['value'] for header in headers if header['name'].lower() == 'from'),
                        'Unknown Sender'
                    ),
                    'date': next(
                        (header['value'] for header in headers if header['name'].lower() == 'date'),
                        ''
                    ),
                    'snippet': msg_detail.get('snippet', ''),
                    'body_preview': get_message_body(msg_detail['payload'])
                }
                current_batch.append(email_info)

                # バッチサイズに達したらBigQueryに挿入
                if len(current_batch) >= batch_size:
                    print(f"\nBigQueryにデータを転送中... ({len(email_data) + 1}～{len(email_data) + len(current_batch)}件目)")
                    insert_to_bigquery(current_batch)
                    email_data.extend(current_batch)
                    current_batch = []
                    time.sleep(1)  # API制限を考慮
                
                print(f"\r{i}/{len(messages)} 件処理中...", end='')

            except Exception as e:
                print(f"\nメール {i} の処理中にエラーが発生しました: {str(e)}")
                continue

        # 残りのバッチを処理
        if current_batch:
            print(f"\nBigQueryに残りのデータを転送中... ({len(email_data) + 1}～{len(email_data) + len(current_batch)}件目)")
            insert_to_bigquery(current_batch)
            email_data.extend(current_batch)

        print(f"\n処理が完了しました。合計 {len(email_data)} 件のデータを転送しました。")

    except Exception as e:
        print(f"エラーが発生しました: {str(e)}")

if __name__ == '__main__':
    main()
