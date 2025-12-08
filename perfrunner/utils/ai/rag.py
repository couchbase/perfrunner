import re
from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Callable, Iterator, Optional

from logger import logger
from perfrunner.helpers.local import download_raw_github_file
from perfrunner.helpers.misc import read_json
from perfrunner.utils.ai.clients import CouchbaseClient, InferenceClient
from perfrunner.utils.ai.f1_calculator import F1Calculator
from perfrunner.utils.ai.wer_calculator import WERCalculator


def normalise_text(text: str, remove_articles: bool = False) -> str:
    """Apply a series of normalisation steps to standardise text.

    Args:
        text: The text string to normalise
        remove_articles: If True, removes common articles (a, an, the) from text.
    """
    # Convert to lowercase and strip leading/trailing whitespace
    text = text.lower().strip()

    # Remove punctuation (anything that's not a word character or whitespace)
    text = re.sub(r"[^\w\s]", "", text)

    # Optionally remove articles (a, an, the) using word boundaries
    if remove_articles:
        text = re.sub(r"\b(a|an|the)\b", " ", text)

    # Normalise whitespace by collapsing multiple spaces into one
    text = re.sub(r"\s+", " ", text)

    return text.strip()


def tokenise(text: str, remove_articles: bool = False) -> list[str]:
    """Tokenise text into words.

    Args:
        text: The text string to tokenise
        remove_articles: If True, removes common articles during normalisation.
    """
    normalised = normalise_text(text, remove_articles=remove_articles)
    return normalised.split() if normalised else []


class GroundTruthAdapter(ABC):
    """Abstract base class for dataset-specific ground truth data normalisation."""

    def normalise_entries(self, raw_data: dict) -> Iterator[dict]:
        """
        Convert raw ground truth format to normalised question-answer entries.

        Yields:
            Normalised entries with 'question', 'answers' (list), and 'evidence' (list)
        """
        for _, qa_entries in raw_data.items():
            for entry in qa_entries:
                question = entry.get("question", "")
                answers = self.extract_answers_from_entry(entry)
                evidence = self.extract_evidence_from_entry(entry)

                yield {
                    "question": question,
                    "answers": answers,
                    "evidence": evidence,
                }

    @abstractmethod
    def extract_answers_from_entry(self, entry: dict) -> list[str]:
        """Extract answers from a raw entry.

        Args:
            entry: Raw entry dictionary

        Returns:
            List of answer strings
        """
        pass

    @abstractmethod
    def extract_evidence_from_entry(self, entry: dict) -> list[str]:
        """Extract evidence from a raw entry.

        Args:
            entry: Raw entry dictionary

        Returns:
            List of evidence text strings
        """
        pass

    def get_system_prompt(self) -> str:
        """Return system prompt for LLM generation."""
        return (
            "The final answer output should be in the format of 'The answer is: <answer>',"
            " and the <answer> should be concise with no extra explanation."
        )


class PaperDatasetAdapter(GroundTruthAdapter):
    """Adapter for paper ground truth format."""

    def extract_answers_from_entry(self, entry: dict) -> list[str]:
        """Extract answers from paper ground truth entry."""
        answers = []
        raw_answers = entry.get("answers", [])
        if isinstance(raw_answers, list):
            answers = [ans.get("answer", "") for ans in raw_answers if ans.get("answer")]
        return answers

    def extract_evidence_from_entry(self, entry: dict) -> list[str]:
        """Extract evidence from paper ground truth entry."""
        evidence = []
        evidence_list = entry.get("evidence", [])
        for ev in evidence_list:
            raw_evidence_list = ev.get("raw_evidence", [])
            for raw_ev in raw_evidence_list:
                if isinstance(raw_ev, list):
                    evidence.append("".join(raw_ev))
                else:
                    evidence.append(str(raw_ev))
        return evidence

    def get_system_prompt(self) -> str:
        return (
            "You are a scientific researcher, given a section of an academic paper, "
            "please answer the question according to the context of the paper. "
        ) + super().get_system_prompt()


class FetaDatasetAdapter(GroundTruthAdapter):
    """Adapter for Feta ground truth format."""

    def extract_answers_from_entry(self, entry: dict) -> list[str]:
        """Extract answers from feta ground truth entry."""
        if answer := entry.get("answers", ""):
            return [answer]
        return []

    def extract_evidence_from_entry(self, entry: dict) -> list[str]:
        """Extract evidence from feta ground truth entry."""
        evidence = ""
        evidence_list = entry.get("evidence", {}).get("table_array", [])
        for row in evidence_list:
            evidence += " ".join(row) + "\n"
        return [evidence.strip()]

    def get_system_prompt(self) -> str:
        return (
            "Given a section of a document, please answer the question in a natural language and "
            "answer the whole question according to the context. "
        ) + super().get_system_prompt()


class NQDatasetAdapter(GroundTruthAdapter):
    """Adapter for Natural Questions ground truth format."""

    def extract_answers_from_entry(self, entry: dict) -> list[str]:
        """Extract answers from NQ ground truth entry."""
        answers = []
        raw_answers = entry.get("answers", {})
        if isinstance(raw_answers, dict):
            short_answer = raw_answers.get("short_answer", "")
            if short_answer:
                answers = [short_answer]
        return answers

    def extract_evidence_from_entry(self, entry: dict) -> list[str]:
        """Extract evidence from NQ dataset entry."""
        evidence = []
        raw_answers = entry.get("answers", {})
        if isinstance(raw_answers, dict):
            # We use long_answer as evidence for the dataset
            long_answer = raw_answers.get("long_answer", "")
            if long_answer:
                evidence = [long_answer]
        return evidence

    def get_system_prompt(self) -> str:
        return (
            "Given a section of a document, please provide an answer as a summarised short phrase. "
        ) + super().get_system_prompt()


def _make_clients(
    model_endpoint: str,
    api_key: str,
    embedding_model_name: str,
    master_node: str,
    username: str,
    password: str,
    bucket_name: str,
    llm_model_name: Optional[str] = None,
) -> tuple[InferenceClient, CouchbaseClient]:
    inference_client = InferenceClient(
        model_endpoint=model_endpoint,
        api_key=api_key,
        embedding_model_name=embedding_model_name,
        llm_model_name=llm_model_name,
    )
    couchbase_client = CouchbaseClient(
        master_node=master_node,
        username=username,
        password=password,
        bucket_name=bucket_name,
    )
    return inference_client, couchbase_client


def _process_wer_task(
    task_data: dict,
    model_endpoint: str,
    api_key: str,
    embedding_model_name: str,
    master_node: str,
    username: str,
    password: str,
    bucket_name: str,
) -> dict:
    """Process a single WER calculation task in parallel.

    Args:
        task_data: Dictionary containing 'evidence_text' and 'top_k'
        model_endpoint: Model endpoint URL
        api_key: API key for inference client
        embedding_model_name: Name of embedding model
        master_node: Couchbase master node
        username: Couchbase username
        password: Couchbase password
        bucket_name: Couchbase bucket name

    Returns:
        WER calculation result dictionary
    """
    evidence_text = task_data["evidence_text"]
    top_k = task_data["top_k"]

    # Create clients for this worker
    inference_client, couchbase_client = _make_clients(
        model_endpoint, api_key, embedding_model_name, master_node, username, password, bucket_name
    )

    # Retrieve documents
    query_embedding = inference_client.get_embeddings(evidence_text)
    if not query_embedding:
        return {"found": False, "wer": 1.0, "substitutions": 0, "insertions": 0, "deletions": 0}
    docs_text = couchbase_client.get_search_text(query_embedding, limit=top_k)

    # Calculate WER
    wer_result = WERCalculator.calculate_wer_for_evidence(
        tokenise(evidence_text),
        tokenise(docs_text),
    )
    return wer_result


def _process_f1_task(
    task_data: dict,
    model_endpoint: str,
    api_key: str,
    embedding_model_name: str,
    llm_model_name: str,
    master_node: str,
    username: str,
    password: str,
    bucket_name: str,
) -> dict:
    """Process a single F1 calculation task in parallel.

    Args:
        task_data: contains 'question', 'expected_answers', 'evidence', 'system_prompt', and 'top_k'
        model_endpoint: Model endpoint URL
        api_key: API key for inference client
        embedding_model_name: Name of embedding model
        llm_model_name: Name of LLM model
        master_node: Couchbase master node
        username: Couchbase username
        password: Couchbase password
        bucket_name: Couchbase bucket name

    Returns:
        F1 calculation result dictionary
    """
    question = task_data["question"]
    expected_answers = task_data["expected_answers"]
    top_k = task_data["top_k"]
    evidence = task_data["evidence"]
    system_prompt = task_data["system_prompt"]
    # Create clients for this worker
    inference_client, couchbase_client = _make_clients(
        model_endpoint=model_endpoint,
        api_key=api_key,
        embedding_model_name=embedding_model_name,
        llm_model_name=llm_model_name,
        master_node=master_node,
        username=username,
        password=password,
        bucket_name=bucket_name,
    )

    # Retrieve documents - use first evidence if available, otherwise use question
    query_text = evidence[0] if evidence and len(evidence) > 0 else question
    query_embedding = inference_client.get_embeddings(query_text)
    if not query_embedding:
        return {"found": False, "f1": 0.0}

    # Get context from retrieved documents
    context = couchbase_client.get_search_text(query_embedding, limit=top_k)

    # Generate LLM answer
    user_prompt = f""" ### Context: {context} \n ### Question: {question} \n ### Response:"""
    llm_answer = inference_client.generate_text(
        system_prompt=system_prompt, user_prompt=user_prompt
    ).split("The answer is:")[-1]

    if not llm_answer:
        return {"found": False, "f1": 0.0}

    # Calculate F1 score against all expected answers and take the best one
    best_f1 = 0.0
    for expected_answer in expected_answers:
        f1_score = F1Calculator.f1_score(tokenise(llm_answer), tokenise(expected_answer))
        best_f1 = max(best_f1, f1_score)

    return {"found": True, "f1": best_f1}


def _run_parallel_calculation(
    tasks: list[dict],
    worker_func: Callable,
    max_workers: int = 10,
    **worker_kwargs,
) -> list[dict]:
    """Run calculations in parallel using ThreadPoolExecutor.

    Args:
        tasks: List of task dictionaries to process
        worker_func: Worker function to process each task
        max_workers: Maximum number of concurrent workers
        **worker_kwargs: Additional keyword arguments to pass to worker function
    """
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        # Submit all tasks
        future_to_task = {
            executor.submit(worker_func, task, **worker_kwargs): task for task in tasks
        }

        # Collect results as they complete
        for future in as_completed(future_to_task):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                task = future_to_task[future]
                logger.error(f"Error processing task {task}: {e}")
                # Add a failure result based on worker function type
                if "wer" in str(worker_func.__name__):
                    results.append(
                        {
                            "found": False,
                            "wer": 1.0,
                            "substitutions": 0,
                            "insertions": 0,
                            "deletions": 0,
                        }
                    )
                else:
                    results.append({"found": False, "f1": 0.0})

    return results


class SimpleRAG:
    """A simple (plain) RAG client that supports basic retrieval operations.

    It encapsulates the retrieval logic and provides a simple interface for retrieving documents
    from the vector database.
    """

    def __init__(
        self,
        model_endpoint: str,
        master_node: str,
        api_key: str,
        embedding_model_name: str,
        credentials: tuple[str, str],
        bucket_name: str,
        gt_file_path: str,
        llm_model_name: Optional[str] = None,
    ):
        # Store connection info for parallel processing
        self.model_endpoint = model_endpoint
        self.api_key = api_key
        self.embedding_model_name = embedding_model_name
        self.llm_model_name = llm_model_name
        self.master_node = master_node
        self.username = credentials[0]
        self.password = credentials[1]
        self.bucket_name = bucket_name
        self.gt_file_name = download_raw_github_file(file_path=gt_file_path)
        raw_data = read_json(self.gt_file_name)

        # Detect dataset type from filename and create appropriate adapter
        if "paper" in gt_file_path:
            self.adapter = PaperDatasetAdapter()
        elif "feta" in gt_file_path:
            self.adapter = FetaDatasetAdapter()
        elif "nq" in gt_file_path:
            self.adapter = NQDatasetAdapter()
        else:
            raise ValueError(f"Unknown dataset type. Could not detect from path: {gt_file_path}")

        # Normalise entries using adapter
        self.qa_entries = list(self.adapter.normalise_entries(raw_data))

    def run_wer_calculation(self, max_workers: int = 20) -> dict:
        """Run WER calculation for the retrieved documents in parallel."""
        logger.info(f"Starting WER calculation for {self.gt_file_name}...")

        # Prepare tasks for parallel processing
        tasks = []
        for entry in self.qa_entries:
            # Extract evidence texts from normalised entry
            evidence_texts = entry.get("evidence", [])
            for evidence_text in evidence_texts:
                # I found that using top_k=2 gives the best results because the evidence text
                # is often few sentences long. Top 2 documents are enough to get the best match
                # even if the chunks break the content into multiple documents.
                tasks.append({"evidence_text": evidence_text, "top_k": 2})

        if not tasks:
            return {
                "total_queries": 0,
                "found_queries": 0,
                "found_rate": 0.0,
                "avg_wer": 1.0,
                "min_wer": 1.0,
                "max_wer": 1.0,
            }

        # Prepare worker function arguments
        worker_kwargs = {
            "model_endpoint": self.model_endpoint,
            "api_key": self.api_key,
            "embedding_model_name": self.embedding_model_name,
            "master_node": self.master_node,
            "username": self.username,
            "password": self.password,
            "bucket_name": self.bucket_name,
        }

        # Run parallel calculations
        wer_results = _run_parallel_calculation(
            tasks, _process_wer_task, max_workers=max_workers, **worker_kwargs
        )

        # Calculate summary statistics
        found_results = [r for r in wer_results if r["found"]]
        wer_scores = [r["wer"] for r in found_results]

        summary = {
            "total_queries": len(wer_results),
            "found_queries": len(found_results),
            "found_rate": len(found_results) / len(wer_results) if wer_results else 0.0,
            "avg_wer": sum(wer_scores) / len(wer_scores) if wer_scores else 1.0,
            "min_wer": min(wer_scores) if wer_scores else 1.0,
            "max_wer": max(wer_scores) if wer_scores else 1.0,
        }
        return summary

    def run_f1_calculation(self, max_workers: int = 15) -> dict:
        """Run F1 calculation for RAG-based QA in parallel.

        For each question in the ground truth:
        1. Retrieve relevant documents using the question
        2. Generate an LLM answer using the retrieved documents as context
        3. Compare the LLM answer to all ground truth answers and take the best F1 score

        Args:
            max_workers: Maximum number of concurrent workers

        Returns:
            Dictionary containing F1 calculation summary statistics
        """
        logger.info(f"Starting F1 calculation for {self.gt_file_name}...")

        # Prepare tasks for parallel processing
        tasks = []
        for entry in self.qa_entries:
            question = entry.get("question", "")
            if not question:
                continue

            # Ideally top_k should be big enough to get enough context for the LLM,
            # but also be able to fit in the LLM context window.
            tasks.append(
                {
                    "question": question,
                    "expected_answers": entry.get("answers", []),
                    "evidence": entry.get("evidence", []),
                    "top_k": 10,
                    "system_prompt": self.adapter.get_system_prompt(),
                }
            )

        if not tasks:
            return {
                "total_queries": 0,
                "found_queries": 0,
                "found_rate": 0.0,
                "avg_f1": 0.0,
                "min_f1": 0.0,
                "max_f1": 0.0,
            }

        # Prepare worker function arguments
        worker_kwargs = {
            "model_endpoint": self.model_endpoint,
            "api_key": self.api_key,
            "embedding_model_name": self.embedding_model_name,
            "llm_model_name": self.llm_model_name,
            "master_node": self.master_node,
            "username": self.username,
            "password": self.password,
            "bucket_name": self.bucket_name,
        }

        # Run parallel calculations
        f1_results = _run_parallel_calculation(
            tasks, _process_f1_task, max_workers=max_workers, **worker_kwargs
        )

        # Calculate summary statistics
        found_results = [r for r in f1_results if r["found"]]
        f1_scores = [r["f1"] for r in found_results]

        summary = {
            "total_queries": len(f1_results),
            "found_queries": len(found_results),
            "found_rate": len(found_results) / len(f1_results) if f1_results else 0.0,
            "avg_f1": sum(f1_scores) / len(f1_scores) if f1_scores else 0.0,
            "min_f1": min(f1_scores) if f1_scores else 0.0,
            "max_f1": max(f1_scores) if f1_scores else 0.0,
        }
        return summary
