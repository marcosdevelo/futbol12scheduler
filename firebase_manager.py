from dotenv import load_dotenv
import os
import firebase_admin
from firebase_admin import credentials, firestore

from logging_config import configure_logging

PATH_TO_KEY = "./futbol12-78ec4-firebase-adminsdk-4oiqb-02baee2767.json"


class FirestoreManager:
    def __init__(self):
        if not firebase_admin._apps:
            cred = credentials.Certificate(PATH_TO_KEY)
            firebase_admin.initialize_app(cred)
        self.db = firestore.client()
        self.logger = configure_logging()

    def update_data(self, collection_name, document_id, data):
        """
        Updates data in a specific document in a Firestore collection.
        """
        try:
            doc_ref = self.db.collection(collection_name).document(document_id)
            doc_ref.update(data)
        except Exception as e:
            self.logger.error(f"Failed to update data in Firestore: {e}")

    def add_data(self, collection_name, document_id, data):
        """
        Adds data to a specific document in a Firestore collection.
        """
        try:
            doc_ref = self.db.collection(collection_name).document(document_id)
            doc_ref.set(data)
        except Exception as e:
            self.logger.error(f"Failed to add data to Firestore: {e}")

    # def read_data(self, collection_name, document_id):
    #     """
    #     Reads data from a specific document in a Firestore collection.
    #     """
    #     doc_ref = self.db.collection(collection_name).document(document_id)
    #     doc = doc_ref.get()
    #     if doc.exists:
    #         return doc.to_dict()
    #     else:
    #         return None
    #
    # def delete_data(self, collection_name, document_id):
    #     """
    #     Deletes a specific document from a Firestore collection.
    #     """
    #     self.db.collection(collection_name).document(document_id).delete()
    #     print(f"Document {document_id} deleted from collection {collection_name}.")
