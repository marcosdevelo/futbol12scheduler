from dotenv import load_dotenv
import os
import firebase_admin
from firebase_admin import credentials, firestore

from logging_config import configure_logging

# Load environment variables from a .env file
load_dotenv()

# Retrieve environment variables
project_id = os.getenv("FIREBASE_PROJECT_ID")
private_key_id = os.getenv("FIREBASE_PRIVATE_KEY_ID")
private_key = os.getenv("FIREBASE_PRIVATE_KEY").replace("\\n", "\n")
client_email = os.getenv("FIREBASE_CLIENT_EMAIL")
client_id = os.getenv("FIREBASE_CLIENT_ID")
auth_uri = os.getenv("FIREBASE_AUTH_URI")
token_uri = os.getenv("FIREBASE_TOKEN_URI")
auth_provider_x509_cert_url = os.getenv("FIREBASE_AUTH_PROVIDER_X509_CERT_URL")
client_x509_cert_url = os.getenv("FIREBASE_CLIENT_X509_CERT_URL")

# Create a dictionary with the credentials
firebase_credentials = {
    "type": "service_account",
    "project_id": project_id,
    "private_key_id": private_key_id,
    "private_key": private_key,
    "client_email": client_email,
    "client_id": client_id,
    "auth_uri": auth_uri,
    "token_uri": token_uri,
    "auth_provider_x509_cert_url": auth_provider_x509_cert_url,
    "client_x509_cert_url": client_x509_cert_url
}


class FirestoreManager:
    def __init__(self):
        if not firebase_admin._apps:
            cred = credentials.Certificate(firebase_credentials)
            firebase_admin.initialize_app(cred)
        self.db = firestore.client()
        self.logger = configure_logging()

    def update_data(self, collection_name, document_id, data):
        """
        Updates data in a specific document in a Firestore collection.
        If the document doesn't exist, it will be created.
        Uses set() with merge=True instead of update() to properly handle empty arrays.
        """
        try:
            doc_ref = self.db.collection(collection_name).document(document_id)
            doc = doc_ref.get()
            
            if doc.exists:
                # Document exists, use set with merge to preserve empty arrays
                doc_ref.set(data, merge=True)
                self.logger.info(f"Updated existing document {document_id} in collection {collection_name}")
            else:
                # Document doesn't exist, create it
                doc_ref.set(data)
                self.logger.info(f"Created new document {document_id} in collection {collection_name}")
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

    def read_data(self, collection_name, document_id):
        """
        Reads data from a specific document in a Firestore collection.
        """
        try:
            doc_ref = self.db.collection(collection_name).document(document_id)
            doc = doc_ref.get()
            if doc.exists:
                return doc.to_dict()
            else:
                return None
        except Exception as e:
            self.logger.error(f"Failed to read data from Firestore: {e}")
            return None
    #
    # def delete_data(self, collection_name, document_id):
    #     """
    #     Deletes a specific document from a Firestore collection.
    #     """
    #     self.db.collection(collection_name).document(document_id).delete()
    #     print(f"Document {document_id} deleted from collection {collection_name}.")
