from collections import Counter


class F1Calculator:
    """Calculate F1 scores for text comparison.

    This class provides methods for calculating F1 scores between predictions and ground truth text,
    with normalisation to handle variations in formatting and punctuation.

    F1 score is the harmonic mean of precision and recall, it is calculated as:
    F1 = 2 * (precision * recall) / (precision + recall)

    Precision measures how many predicted tokens are correct.
    Recall measures how many ground truth tokens were found in prediction.
    """

    @staticmethod
    def f1_score(pred_tokens: list[str], truth_tokens: list[str]) -> float:
        """Calculate F1 score between prediction and ground truth.

        The method handles special cases for yes/no questions and empty inputs.

        Args:
            prediction: The predicted text to evaluate
            ground_truth: The correct/expected text

        Returns:
            F1 score as a float between 0.0 and 1.0, where:
            - 1.0 indicates perfect match
            - 0.0 indicates no overlap
        """
        # Handle edge case: empty ground truth
        if len(truth_tokens) == 0:
            # Perfect match if both are empty, otherwise no match
            return 1.0 if len(pred_tokens) == 0 else 0.0

        # Handle edge case: empty prediction
        if len(pred_tokens) == 0:
            return 0.0

        # Special handling for yes/no questions
        if truth_tokens[0] in ["yes", "no"]:
            if truth_tokens[0] == pred_tokens[0]:
                return 1.0

        # Calculate token overlap using Counter intersection
        # This counts matching tokens, accounting for duplicates
        common_tokens = Counter(truth_tokens) & Counter(pred_tokens)
        common_token_count = sum(common_tokens.values())

        # If no tokens match, F1 score is 0.0
        if common_token_count == 0:
            return 0.0

        # Calculate precision: proportion of predicted tokens that are correct
        precision = 1.0 * common_token_count / len(pred_tokens)

        # Calculate recall: proportion of ground truth tokens found in prediction
        recall = 1.0 * common_token_count / len(truth_tokens)
        f1 = F1Calculator._f1_score(precision, recall)

        # If score is below a threshold, try to do paragraph-level matching
        if f1 < 0.5:
            f1 = max(f1, F1Calculator.paragraph_f1_score(pred_tokens, truth_tokens))
        return f1

    @staticmethod
    def paragraph_f1_score(pred_tokens: list[str], truth_tokens: list[str]) -> float:
        """Calculate paragraph F1 score between prediction and ground truth.

        This method uses set-based token matching (ignoring duplicates) rather
        than counting token frequencies. This is useful for paragraph-level
        comparisons where we care about unique token presence rather than
        token frequency.

        Args:
            pred_tokens: The predicted tokens to evaluate
            truth_tokens: The correct/expected tokens

        Returns:
            F1 score as a float between 0.0 and 1.0
        """
        # Count unique matching tokens (set intersection, ignoring duplicates)
        unique_common_tokens = set(truth_tokens).intersection(set(pred_tokens))
        unique_common_count = len(unique_common_tokens)

        # If no unique tokens match, F1 score is 0.0
        if unique_common_count == 0:
            return 0.0

        # Calculate precision and recall using unique token counts
        precision = unique_common_count / len(pred_tokens)
        recall = unique_common_count / len(truth_tokens)

        return F1Calculator._f1_score(precision, recall)

    @staticmethod
    def _f1_score(precision: float, recall: float) -> float:
        # Calculate F1 score: harmonic mean of precision and recall
        # Harmonic mean gives more weight to lower values, penalising imbalance
        return (2 * precision * recall) / (precision + recall)
