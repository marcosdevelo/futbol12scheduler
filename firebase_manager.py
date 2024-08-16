from dotenv import load_dotenv
import os
import firebase_admin
from firebase_admin import credentials, firestore

from f12scheduler.logging_config import configure_logging


class FirestoreManager:
    def __init__(self):
        # Load environment variables from .env file
        load_dotenv()

        # Get the path to the service account JSON key from the environment variable
        credentials_path = os.getenv("GOOGLE_APPLICATION_CREDENTIALS")

        # Initialize the Firebase Admin SDK with the credentials
        cred = credentials.Certificate(credentials_path)
        firebase_admin.initialize_app(cred)

        # Initialize Firestore
        self.db = firestore.client()
        self.logger = configure_logging()

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
    # def update_data(self, collection_name, document_id, data):
    #     """
    #     Updates data in a specific document in a Firestore collection.
    #     """
    #     doc_ref = self.db.collection(collection_name).document(document_id)
    #     doc_ref.update(data)
    #     print(f"Document {document_id} updated in collection {collection_name}.")
    #
    # def delete_data(self, collection_name, document_id):
    #     """
    #     Deletes a specific document from a Firestore collection.
    #     """
    #     self.db.collection(collection_name).document(document_id).delete()
    #     print(f"Document {document_id} deleted from collection {collection_name}.")
