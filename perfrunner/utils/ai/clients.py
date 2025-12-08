import logging
from typing import Optional

import openai
from couchbase.auth import PasswordAuthenticator
from couchbase.cluster import Cluster
from couchbase.exceptions import CouchbaseException
from couchbase.options import ClusterOptions

from logger import logger

# Configure OpenAI SDK (including httpcore and httpx) logging to suppress INFO level logs
for logger_name in ["openai", "httpcore", "httpx"]:
    http_logger = logging.getLogger(logger_name)
    http_logger.setLevel(logging.WARNING)
    http_logger.propagate = False


class CouchbaseClient:
    """Client for interacting with Couchbase server for vector search operations."""

    def __init__(
        self,
        master_node: str,
        username: str,
        password: str,
        bucket_name: str,
    ):
        try:
            connection_string = f"couchbases://{master_node}?ssl=no_verify"

            auth = PasswordAuthenticator(username, password)
            cluster_options = ClusterOptions.create_options_with_profile(auth, "wan_development")
            self.cluster = Cluster(connection_string, cluster_options)
            self.bucket = self.cluster.bucket(bucket_name)
            self.bucket_name = bucket_name
        except CouchbaseException as e:
            logger.interrupt(f"Failed to connect to Couchbase: {e}")

    def search_vector(self, query_embedding: list[float], limit: int = 1) -> list[dict]:
        try:
            select_query = (
                f"SELECT meta().id, `text-to-embed` "
                f"FROM `{self.bucket_name}` "
                f"ORDER BY ANN_DISTANCE(`text-embedding`, {query_embedding}, 'L2') "
                f"LIMIT {limit};"
            )

            query_result = self.cluster.query(select_query).execute()
            results = [row for row in query_result]

            return results
        except CouchbaseException as e:
            logger.error(f"Vector search failed: {e}")
            return []

    def get_search_text(self, query_embedding: list[float], limit: int = 1) -> str:
        try:
            results = self.search_vector(query_embedding, limit)
            return " ".join(doc.get("text-to-embed", "") for doc in results)
        except CouchbaseException as e:
            logger.error(f"Failed to get search text: {e}")
            return ""


class InferenceClient:
    """Client for interacting with inference server for embedding and LLM operations."""

    def __init__(
        self,
        model_endpoint: str,
        api_key: str,
        embedding_model_name: str,
        llm_model_name: Optional[str] = None,
    ):
        self.model_endpoint = model_endpoint
        self.api_key = api_key
        self.embedding_model_name = embedding_model_name
        self.llm_model_name = llm_model_name

        self.client = openai.OpenAI(
            base_url=f"{model_endpoint}/v1",
            api_key=api_key,
        )

    def get_embeddings(self, text: str) -> list[float]:
        try:
            response = self.client.embeddings.create(
                input=text,
                model=self.embedding_model_name,
            )
            return response.data[0].embedding
        except Exception as e:
            logger.error(f"Failed to get embeddings: {e}")
            return []

    def generate_text(self, system_prompt: str, user_prompt: str) -> str:
        try:
            response = self.client.chat.completions.create(
                model=self.llm_model_name,
                messages=[
                    {"role": "system", "content": system_prompt},
                    {"role": "user", "content": user_prompt},
                ],
            )
            return response.choices[0].message.content
        except Exception as e:
            logger.error(f"Failed to generate text: {e}")
            return ""
