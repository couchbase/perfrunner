from logger import logger


class WERCalculator:
    """Calculate Word Error Rate (WER) for text comparison.

    WER measures the edit distance between reference (ground truth) and retrieved_text.
    WER of 0.0 means perfect match, 1.0 means complete mismatch.
    """

    @staticmethod
    def calculate_wer(
        ref_words: list[str], retrieved_words: list[str]
    ) -> tuple[float, int, int, int]:
        """
        Calculate Word Error Rate (WER) by finding best matching subsequence.

        When retrieved_text is longer than reference, we find the best matching subsequence in the
        retrieved_text to calculate WER. This is because when we query the documents, we retrieve
        all the documents that match the query, and depending on factors such as workflow chunking
        strategy and size, and k value we endup with more text than what the ground truth may have
        contained. This prevents us from penalising the extra text that was retrieved.

        Args:
            ref_words: The ground truth words
            retrieved_words: The words retrieved from the server to compare against reference words

        Returns:
            Tuple of (WER, substitutions, insertions, deletions)
            - WER: Word Error Rate (0.0 to 1.0, where 0.0 is perfect match)
            - substitutions: Number of word substitutions needed
            - insertions: Number of word insertions needed
            - deletions: Number of word deletions needed
        """
        # Handle edge cases: empty reference words or retrieved words
        if len(ref_words) == 0:
            # If reference is empty, WER is 0, as we have no ground truth to compare against
            return 0.0, 0, 0, 0
        if len(retrieved_words) == 0:
            # If retrieved words are empty, WER is 1.0
            return 1.0, 0, 0, len(ref_words)

        # If retrieved words are shorter or equal length, use standard WER calculation
        if len(retrieved_words) <= len(ref_words):
            return WERCalculator._calculate_standard_wer(ref_words, retrieved_words)

        # Retrieved words are longer than reference words: find best matching subsequence first
        # and then remove extra words at the beginning/end
        best_wer = 1.0
        best_substitutions = best_insertions = best_deletions = 0

        # Try all possible starting positions for a subsequence of reference length
        # This finds the best alignment between reference words and retrieved words
        for start_position in range(len(retrieved_words) - len(ref_words) + 1):
            # Extract a subsequence from retrieved words with same length as reference words
            end_position = start_position + len(ref_words)
            retrieved_subsequence = retrieved_words[start_position:end_position]

            # Calculate WER for this subsequence alignment
            wer, substitutions, insertions, deletions = WERCalculator._calculate_standard_wer(
                ref_words, retrieved_subsequence
            )

            # Track the alignment with the lowest WER (best match)
            if wer < best_wer:
                best_wer = wer
                best_substitutions = substitutions
                best_insertions = insertions
                best_deletions = deletions

        return best_wer, best_substitutions, best_insertions, best_deletions

    @staticmethod
    def _calculate_standard_wer(
        ref_words: list[str], retrieved_words: list[str]
    ) -> tuple[float, int, int, int]:
        """
        Calculate standard WER between two word lists.

        Uses the Levenshtein distance algorithm to find the minimum edit distance
        between reference (ground truth) and retrieved_words (from server) sequences.
        The edit distance matrix tracks the minimum number of operations
        (substitutions, insertions, deletions) needed to transform retrieved_words
        into reference.

        Args:
            ref_words: List of words in the reference (ground truth) text
            retrieved_words: List of words in the retrieved text

        Returns:
            Tuple of (WER, substitutions, insertions, deletions)
        """
        # Matrix dimensions: (len(ref_words) + 1) x (len(retrieved_words) + 1)
        # distance_matrix[ref_idx][retrieved_idx] = minimum edit distance to align
        # first ref_idx words of reference with first retrieved_idx words of retrieved_text
        matrix_rows = len(ref_words) + 1
        matrix_cols = len(retrieved_words) + 1
        distance_matrix = [[0] * matrix_cols for _ in range(matrix_rows)]

        # Initialize base cases: first row and column
        # First column: transforming empty retrieved_text to reference requires deletions
        for ref_word_idx in range(matrix_rows):
            distance_matrix[ref_word_idx][0] = ref_word_idx
        # First row: transforming retrieved_text to empty reference requires insertions
        for retrieved_word_idx in range(matrix_cols):
            distance_matrix[0][retrieved_word_idx] = retrieved_word_idx

        # For each position, find the minimum cost operation
        for ref_word_idx in range(1, matrix_rows):
            for retrieved_word_idx in range(1, matrix_cols):
                # If words match, no operation needed (carry forward previous distance)
                if ref_words[ref_word_idx - 1] == retrieved_words[retrieved_word_idx - 1]:
                    prev_distance = distance_matrix[ref_word_idx - 1][retrieved_word_idx - 1]
                    distance_matrix[ref_word_idx][retrieved_word_idx] = prev_distance
                else:
                    # Words don't match: choose minimum cost operation
                    # Option 1: Delete word from reference (move up in reference)
                    deletion_cost = distance_matrix[ref_word_idx - 1][retrieved_word_idx] + 1
                    # Option 2: Insert word into retrieved_text (move right in retrieved_text)
                    insertion_cost = distance_matrix[ref_word_idx][retrieved_word_idx - 1] + 1
                    # Option 3: Substitute word (diagonal move)
                    substitution_cost = (
                        distance_matrix[ref_word_idx - 1][retrieved_word_idx - 1] + 1
                    )
                    # Take the minimum cost operation
                    distance_matrix[ref_word_idx][retrieved_word_idx] = min(
                        deletion_cost, insertion_cost, substitution_cost
                    )

        # Extract total edit distance and word count
        total_reference_words = len(ref_words)
        total_edit_distance = distance_matrix[-1][-1]

        # Backtrack through the matrix to count actual edit operations
        # This determines how many substitutions, insertions, and deletions occurred
        substitution_count = insertion_count = deletion_count = 0
        ref_word_idx, retrieved_word_idx = len(ref_words), len(retrieved_words)

        # Trace back from bottom-right to top-left of the matrix
        while ref_word_idx > 0 and retrieved_word_idx > 0:
            current_distance = distance_matrix[ref_word_idx][retrieved_word_idx]

            # Check if words match (no operation needed)
            if ref_words[ref_word_idx - 1] == retrieved_words[retrieved_word_idx - 1]:
                # Move diagonally (both indices decrease)
                ref_word_idx -= 1
                retrieved_word_idx -= 1
            # Check if substitution was used (diagonal move with cost +1)
            elif current_distance == distance_matrix[ref_word_idx - 1][retrieved_word_idx - 1] + 1:
                substitution_count += 1
                ref_word_idx -= 1
                retrieved_word_idx -= 1
            # Check if deletion was used (move up in reference)
            elif current_distance == distance_matrix[ref_word_idx - 1][retrieved_word_idx] + 1:
                deletion_count += 1
                ref_word_idx -= 1
            # Otherwise, insertion was used (move right in retrieved_text)
            else:
                insertion_count += 1
                retrieved_word_idx -= 1

        # Handle remaining words: if we haven't reached the start of reference/retrieved_text
        # Remaining reference words are deletions, remaining retrieved_text words are insertions
        deletion_count += ref_word_idx
        insertion_count += retrieved_word_idx

        # Calculate WER: edit_distance / total_words, capped at 1.0
        if total_reference_words > 0:
            wer = min(total_edit_distance / total_reference_words, 1.0)
        else:
            wer = 1.0
        return wer, substitution_count, insertion_count, deletion_count

    @staticmethod
    def calculate_wer_for_evidence(evidence_text: list[str], retrieved_text: list[str]) -> dict:
        """Calculate WER for a single evidence text by comparing it to the retrieved text.

        Args:
            evidence_text: Ground truth evidence words to compare against
            retrieved_text: List of retrieved words to compare against evidence words

        Returns:
            Dictionary containing WER calculation results
        """
        if not retrieved_text:
            logger.warning("No retrieved text found")
            return {
                "found": False,
                "wer": 1.0,
                "substitutions": 0,
                "insertions": 0,
                "deletions": 0,
            }

        # Calculate WER between evidence words and retrieved words
        wer, substitutions, insertions, deletions = WERCalculator.calculate_wer(
            evidence_text, retrieved_text
        )

        return {
            "found": True,
            "wer": wer,
            "substitutions": substitutions,
            "insertions": insertions,
            "deletions": deletions,
        }
