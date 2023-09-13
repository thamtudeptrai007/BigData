import csv
import firebase_admin
from firebase_admin import credentials
from firebase_admin import firestore
import pandas as pd

def update():
    # Đường dẫn đến tệp serviceAccountKey.json
    cred = credentials.Certificate('C:/Users/admin/Desktop/BigData/bigdata-eec24-firebase-adminsdk-n4a82-94ef80532e.json')

    if not firebase_admin._apps:
        # Khởi tạo ứng dụng Firebase
        firebase_admin.initialize_app(cred)

    db = firestore.client()

    df2 = pd.read_csv('C:/Users/admin/Desktop/BigData/spark_process/output/top_20_country.csv')

    # Đường dẫn đến tệp CSV
    data2 = df2.to_dict(orient='records')

    collection_ref2 = db.collection('top_20_country')

    for record in data2:
        # Tìm tất cả các tài liệu trong collection có trường 'name' trùng với 'name' từ CSV
        query2 = collection_ref2.where('country', '==', record['country'])
        docs2 = query2.stream()

        # Cập nhật trường 'count' của các tài liệu tìm thấy
        for doc in docs2:
            doc.reference.update({'count': record['count']})

    existing_names2 = set()
    docs5 = collection_ref2.stream()
    for doc in docs5:
        existing_names2.add(doc.get('country'))
    docs_to_delete = []
    for name in existing_names2:
        if name not in [record['country'] for record in data2]:
            query = collection_ref2.where('country', '==', name)
            docs = query.stream()
            for doc in docs:
                docs_to_delete.append(doc.reference)

    # Xóa các tài liệu không cần thiết
    for doc_ref in docs_to_delete:
        doc_ref.delete()

    # Thêm các tài liệu mới cho những 'record[name]' chưa tồn tại trong Firestore
    for record in data2:
        if record['country'] not in existing_names2:
            collection_ref2.add(record)

    df3 = pd.read_csv('C:/Users/admin/Desktop/BigData/spark_process/output/top_20_category.csv')

    # Đường dẫn đến tệp CSV
    data3 = df3.to_dict(orient='records')


    collection_ref3 = db.collection('top_20_category')

    for record in data3:
        # Tìm tất cả các tài liệu trong collection có trường 'name' trùng với 'name' từ CSV
        query3 = collection_ref3.where('category', '==', record['category'])
        docs3 = query3.stream()

        # Cập nhật trường 'count' của các tài liệu tìm thấy
        for doc in docs3:
            doc.reference.update({'pledged': record['pledged']})

    existing_names3 = set()
    docs6 = collection_ref3.stream()
    for doc in docs6:
        existing_names3.add(doc.get('category'))
    docs_to_delete = []
    for name in existing_names3:
        if name not in [record['category'] for record in data3]:
            query = collection_ref3.where('category', '==', name)
            docs = query.stream()
            for doc in docs:
                docs_to_delete.append(doc.reference)

    # Xóa các tài liệu không cần thiết
    for doc_ref in docs_to_delete:
        doc_ref.delete()

    # Thêm các tài liệu mới cho những 'record[name]' chưa tồn tại trong Firestore
    for record in data3:
        if record['category'] not in existing_names3:
            collection_ref3.add(record)