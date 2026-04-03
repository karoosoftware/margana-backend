from __future__ import annotations

from collections import Counter
from dataclasses import dataclass, field
from datetime import date
from typing import Any, Iterable, Protocol

from margana_gen.generator_difficulty import band_for_total_score


@dataclass(frozen=True)
class ValidationIssue:
    code: str
    level: str
    message: str
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class ValidationResult:
    issues: list[ValidationIssue] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not any(issue.level == "error" for issue in self.issues)

    def add(self, issues: Iterable[ValidationIssue]) -> None:
        self.issues.extend(issues)


@dataclass
class PuzzleValidationContext:
    completed_payload: dict[str, Any]
    semi_completed_payload: dict[str, Any] | None = None
    letter_scores: dict[str, int] = field(default_factory=dict)
    horizontal_exclude_words: set[str] = field(default_factory=set)

    @property
    def meta(self) -> dict[str, Any]:
        return self.completed_payload.get("meta") or {}

    @property
    def grid_rows(self) -> list[str]:
        return list(self.completed_payload.get("grid_rows") or [])

    @property
    def valid_words_metadata(self) -> list[dict[str, Any]]:
        return list(self.completed_payload.get("valid_words_metadata") or [])

    @property
    def total_score(self) -> int:
        try:
            return int(self.completed_payload.get("total_score") or 0)
        except Exception:
            return 0


class ValidationRule(Protocol):
    name: str

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        ...


def run_validations(ctx: PuzzleValidationContext, rules: Iterable[ValidationRule]) -> ValidationResult:
    result = ValidationResult()
    for rule in rules:
        result.add(rule.validate(ctx))
    return result


def run_collection_validations(contexts: Iterable[PuzzleValidationContext]) -> ValidationResult:
    contexts_list = list(contexts)
    result = ValidationResult()
    result.add(validate_madness_per_complete_iso_week(contexts_list))
    return result


def validate_madness_per_complete_iso_week(contexts: Iterable[PuzzleValidationContext]) -> list[ValidationIssue]:
    weeks: dict[tuple[int, int], list[PuzzleValidationContext]] = {}
    for ctx in contexts:
        meta = ctx.meta
        date_str = str(meta.get("date") or "").strip()
        if not date_str:
            continue
        try:
            week_date = date.fromisoformat(date_str)
        except ValueError:
            continue
        iso = week_date.isocalendar()
        weeks.setdefault((iso.year, iso.week), []).append(ctx)

    issues: list[ValidationIssue] = []
    for (iso_year, iso_week), week_contexts in sorted(weeks.items()):
        if len(week_contexts) != 7:
            continue
        madness_count = sum(1 for ctx in week_contexts if ctx.meta.get("madnessAvailable") is True)
        if madness_count != 1:
            issues.append(
                ValidationIssue(
                    code="weekly_madness_count_mismatch",
                    level="error",
                    message=f"ISO week {iso_year}-W{iso_week:02d} has {madness_count} madness puzzles; expected exactly 1",
                    details={"iso_year": iso_year, "iso_week": iso_week, "madness_count": madness_count},
                )
            )
    return issues


def default_rules(*, min_anagram_len: int = 8, max_anagram_len: int = 10) -> list[ValidationRule]:
    return [
        GridShapeRule(),
        ValidWordsRowsConsistencyRule(),
        HorizontalExcludeRule(),
        TopLevelTargetConsistencyRule(),
        LongestAnagramCountRule(),
        ColumnTargetRule(),
        DiagonalTargetRule(),
        AnagramLengthRule(min_len=min_anagram_len, max_len=max_anagram_len),
        AnagramMetadataConsistencyRule(),
        AnagramLetterInventoryRule(),
        MadnessConsistencyRule(),
        MadnessPathRule(),
        SemiCompletedConsistencyRule(),
        DifficultyBandConsistencyRule(),
        FixedTargetExclusionRule(),
        BonusAlwaysZeroRule(),
        TotalScoreRule(),
    ]


def rules_for_preset(
    preset: str,
    *,
    min_anagram_len: int = 8,
    max_anagram_len: int = 10,
) -> list[ValidationRule]:
    preset_norm = _norm(preset) or "default"
    if preset_norm in {"default", "strict"}:
        return default_rules(min_anagram_len=min_anagram_len, max_anagram_len=max_anagram_len)
    raise ValueError(f"unknown validation preset: {preset}")


class GridShapeRule:
    name = "grid_shape"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        rows = ctx.grid_rows
        meta = ctx.meta
        if len(rows) != 5:
            issues.append(
                ValidationIssue(
                    code="grid_must_have_five_rows",
                    level="error",
                    message=f"grid_rows must contain exactly 5 rows, found {len(rows)}",
                )
            )
        expected_rows = _int_or_none(meta.get("rows"))
        expected_cols = _int_or_none(meta.get("cols"))
        expected_len = _int_or_none(meta.get("wordLength") or meta.get("word_length"))

        if expected_rows is not None and len(rows) != expected_rows:
            issues.append(
                ValidationIssue(
                    code="grid_row_count_mismatch",
                    level="error",
                    message=f"grid_rows has {len(rows)} rows, expected {expected_rows}",
                )
            )
        if expected_len is not None and expected_cols is not None and expected_len != expected_cols:
            issues.append(
                ValidationIssue(
                    code="grid_meta_inconsistent",
                    level="error",
                    message=f"meta.wordLength={expected_len} does not match meta.cols={expected_cols}",
                )
            )
        expected_width = expected_cols if expected_cols is not None else expected_len
        for idx, row in enumerate(rows):
            if not isinstance(row, str):
                issues.append(
                    ValidationIssue(
                        code="grid_row_not_string",
                        level="error",
                        message=f"grid row {idx} is not a string",
                    )
                )
                continue
            if len(row) != 5:
                issues.append(
                    ValidationIssue(
                        code="grid_row_must_have_five_chars",
                        level="error",
                        message=f"grid row {idx} must have exactly 5 characters, found {len(row)}",
                        details={"row_index": idx, "row": row},
                    )
                )
            if expected_width is not None and len(row) != expected_width:
                issues.append(
                    ValidationIssue(
                        code="grid_row_length_mismatch",
                        level="error",
                        message=f"grid row {idx} has length {len(row)}, expected {expected_width}",
                        details={"row_index": idx, "row": row},
                    )
                )
        return issues


class ValidWordsRowsConsistencyRule:
    name = "valid_words_rows_consistency"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        valid_words = ctx.completed_payload.get("valid_words") or {}
        rows_section = valid_words.get("rows") or {}
        lr_rows = rows_section.get("lr")
        rl_rows = rows_section.get("rl") or []
        if lr_rows is None:
            return []
        issues: list[ValidationIssue] = []
        lr_rows_list = list(lr_rows)
        if lr_rows_list != ctx.grid_rows:
            issues.append(
                ValidationIssue(
                    code="valid_words_rows_lr_mismatch",
                    level="error",
                    message="valid_words.rows.lr does not match grid_rows",
                    details={"grid_rows": ctx.grid_rows, "valid_words_rows_lr": lr_rows},
                )
            )
        reversed_lr_words = {row[::-1] for row in lr_rows_list if isinstance(row, str)}
        for word in rl_rows:
            if word not in reversed_lr_words:
                issues.append(
                    ValidationIssue(
                        code="valid_words_rows_rl_mismatch",
                        level="error",
                        message="valid_words.rows.rl contains a word that is not the reverse of any lr row",
                        details={"word": word, "reversed_lr_words": sorted(reversed_lr_words)},
                    )
                )
        return issues


class HorizontalExcludeRule:
    name = "horizontal_exclude"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        excluded = {w.lower() for w in ctx.horizontal_exclude_words if w}
        if not excluded:
            return []

        issues: list[ValidationIssue] = []
        for idx, row in enumerate(ctx.grid_rows):
            word = _norm(row)
            if word in excluded:
                issues.append(
                    ValidationIssue(
                        code="horizontal_exclude_word_in_grid",
                        level="error",
                        message=f"grid row {idx} is excluded: {word}",
                        details={"row_index": idx, "word": word},
                    )
                )

        valid_words = ctx.completed_payload.get("valid_words") or {}
        rows_section = valid_words.get("rows") or {}
        for bucket in ("lr", "rl"):
            for word in rows_section.get(bucket) or []:
                norm_word = _norm(word)
                if norm_word in excluded:
                    issues.append(
                        ValidationIssue(
                            code="horizontal_exclude_word_in_valid_words",
                            level="error",
                            message=f"valid_words.rows.{bucket} contains excluded word: {norm_word}",
                            details={"bucket": bucket, "word": norm_word},
                        )
                    )
        return issues


class TopLevelTargetConsistencyRule:
    name = "top_level_target_consistency"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        meta = ctx.meta
        issues: list[ValidationIssue] = []
        pairs = [
            ("vertical_target_word", "verticalTargetWord"),
            ("diagonal_target_word", "diagonalTargetWord"),
            ("diagonal_direction", "diagonalDirection"),
        ]
        for top_key, meta_key in pairs:
            top_val = ctx.completed_payload.get(top_key)
            meta_val = meta.get(meta_key)
            if top_val is not None and meta_val is not None and top_val != meta_val:
                issues.append(
                    ValidationIssue(
                        code="target_field_mismatch",
                        level="error",
                        message=f"{top_key} does not match meta.{meta_key}",
                        details={"top_level": top_val, "meta": meta_val},
                    )
                )
        return issues


class ColumnTargetRule:
    name = "column_target"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        meta = ctx.meta
        rows = ctx.grid_rows
        col_idx = _int_or_none(meta.get("columnIndex") or meta.get("column_index"))
        target = _norm(meta.get("verticalTargetWord") or meta.get("vertical_target_word"))
        if col_idx is None or not target or not rows:
            return []
        try:
            actual = "".join(row[col_idx] for row in rows).lower()
        except Exception:
            return [
                ValidationIssue(
                    code="column_target_unreadable",
                    level="error",
                    message="could not extract the target column from grid_rows",
                )
            ]
        if actual != target:
            return [
                ValidationIssue(
                    code="column_target_mismatch",
                    level="error",
                    message=f"column {col_idx} spells '{actual}', expected '{target}'",
                )
            ]
        return []


class LongestAnagramCountRule:
    name = "longest_anagram_count"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        meta = ctx.meta
        longest = _norm(meta.get("longestAnagram") or meta.get("longest_anagram"))
        count = _int_or_none(meta.get("longestAnagramCount") or meta.get("longest_anagram_count"))
        if not longest and count in (None, 0):
            return []
        if longest and count == len(longest):
            return []
        return [
            ValidationIssue(
                code="longest_anagram_count_mismatch",
                level="error",
                message=f"longestAnagramCount={count} does not match len(longestAnagram)={len(longest)}",
                details={"word": longest, "count": count},
            )
        ]


class DiagonalTargetRule:
    name = "diagonal_target"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        meta = ctx.meta
        rows = ctx.grid_rows
        direction = _norm(meta.get("diagonalDirection") or meta.get("diagonal_direction"))
        target = _norm(meta.get("diagonalTargetWord") or meta.get("diagonal_target_word"))
        if direction not in {"main", "anti"} or not target or not rows:
            return []
        try:
            if direction == "main":
                actual = "".join(rows[i][i] for i in range(len(rows))).lower()
            else:
                actual = "".join(rows[i][len(rows) - 1 - i] for i in range(len(rows))).lower()
        except Exception:
            return [
                ValidationIssue(
                    code="diagonal_target_unreadable",
                    level="error",
                    message="could not extract the target diagonal from grid_rows",
                )
            ]
        if actual != target:
            return [
                ValidationIssue(
                    code="diagonal_target_mismatch",
                    level="error",
                    message=f"{direction} diagonal spells '{actual}', expected '{target}'",
                )
            ]
        return []


class MadnessConsistencyRule:
    name = "madness_consistency"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        meta = ctx.meta
        issues: list[ValidationIssue] = []
        available = meta.get("madnessAvailable")
        word = meta.get("madnessWord")
        direction = meta.get("madnessDirection")
        path = meta.get("madnessPath")
        score = meta.get("madnessScore")

        if available is False:
            unexpected = {
                key: value
                for key, value in {
                    "madnessWord": word,
                    "madnessDirection": direction,
                    "madnessPath": path,
                    "madnessScore": score,
                }.items()
                if value is not None
            }
            if unexpected:
                issues.append(
                    ValidationIssue(
                        code="madness_fields_present_when_unavailable",
                        level="error",
                        message="madness fields should be null when madnessAvailable is false",
                        details=unexpected,
                    )
                )
            return issues

        if available is True:
            missing = []
            if not word:
                missing.append("madnessWord")
            if not direction:
                missing.append("madnessDirection")
            if path is None:
                missing.append("madnessPath")
            if score is None:
                missing.append("madnessScore")
            if missing:
                issues.append(
                    ValidationIssue(
                        code="madness_fields_missing_when_available",
                        level="error",
                        message="madness fields are missing while madnessAvailable is true",
                        details={"missing": missing},
                    )
                )
        return issues


class MadnessPathRule:
    name = "madness_path"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        meta = ctx.meta
        if meta.get("madnessAvailable") is not True:
            return []

        word = _norm(meta.get("madnessWord"))
        path = meta.get("madnessPath")
        if not word or path is None:
            return []

        issues: list[ValidationIssue] = []
        if word not in {"margana", "anagram"}:
            issues.append(
                ValidationIssue(
                    code="madness_word_invalid",
                    level="error",
                    message=f"madnessWord must be 'margana' or 'anagram', got '{word}'",
                    details={"madnessWord": word},
                )
            )

        coords = _normalize_madness_path(path)
        if coords is None:
            return [
                ValidationIssue(
                    code="madness_path_invalid_shape",
                    level="error",
                    message="madnessPath must be a list of [row, col] pairs or {'r','c'} objects",
                    details={"madnessPath": path},
                )
            ] + issues

        if len(coords) != len(word):
            issues.append(
                ValidationIssue(
                    code="madness_path_length_mismatch",
                    level="error",
                    message=f"madnessPath length {len(coords)} does not match madnessWord length {len(word)}",
                    details={"madnessWord": word, "madnessPath": path},
                )
            )

        rows = ctx.grid_rows
        seen: set[tuple[int, int]] = set()
        extracted_letters: list[str] = []
        for idx, (row_idx, col_idx) in enumerate(coords):
            if not (0 <= row_idx < len(rows)) or not rows or not (0 <= col_idx < len(rows[row_idx])):
                issues.append(
                    ValidationIssue(
                        code="madness_path_out_of_bounds",
                        level="error",
                        message=f"madnessPath coordinate {idx} is out of bounds",
                        details={"index": idx, "coordinate": [row_idx, col_idx]},
                    )
                )
                continue
            if (row_idx, col_idx) in seen:
                issues.append(
                    ValidationIssue(
                        code="madness_path_reuses_cell",
                        level="error",
                        message=f"madnessPath reuses cell at index {idx}",
                        details={"index": idx, "coordinate": [row_idx, col_idx]},
                    )
                )
            seen.add((row_idx, col_idx))
            extracted_letters.append(rows[row_idx][col_idx].lower())

        for idx in range(1, len(coords)):
            prev_r, prev_c = coords[idx - 1]
            cur_r, cur_c = coords[idx]
            if max(abs(cur_r - prev_r), abs(cur_c - prev_c)) != 1:
                issues.append(
                    ValidationIssue(
                        code="madness_path_not_touching",
                        level="error",
                        message=f"madnessPath coordinates at {idx - 1} and {idx} are not touching",
                        details={"from": [prev_r, prev_c], "to": [cur_r, cur_c]},
                    )
                )

        if extracted_letters and "".join(extracted_letters) != word:
            issues.append(
                ValidationIssue(
                    code="madness_path_word_mismatch",
                    level="error",
                    message="letters extracted from madnessPath do not match madnessWord",
                    details={"madnessWord": word, "pathLetters": "".join(extracted_letters)},
                )
            )

        return issues


class AnagramLengthRule:
    name = "anagram_length"

    def __init__(self, min_len: int = 8, max_len: int = 10):
        self.min_len = min_len
        self.max_len = max_len

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        meta = ctx.meta
        longest = _norm(meta.get("longestAnagram") or meta.get("longest_anagram"))
        if not longest:
            return []
        n = len(longest)
        if self.min_len <= n <= self.max_len:
            return []
        return [
            ValidationIssue(
                code="anagram_length_out_of_range",
                level="error",
                message=f"longestAnagram length {n} outside {self.min_len}-{self.max_len}",
                details={"word": longest},
            )
        ]


class AnagramMetadataConsistencyRule:
    name = "anagram_metadata_consistency"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        longest = _norm(ctx.meta.get("longestAnagram") or ctx.meta.get("longest_anagram"))
        anagram_items = [item for item in ctx.valid_words_metadata if _norm(item.get("type")) == "anagram"]

        if not longest and not anagram_items:
            return []

        issues: list[ValidationIssue] = []
        if not longest and anagram_items:
            issues.append(
                ValidationIssue(
                    code="anagram_metadata_without_longest_anagram",
                    level="error",
                    message="anagram metadata exists but meta.longestAnagram is missing",
                )
            )
            return issues

        if longest and not anagram_items:
            issues.append(
                ValidationIssue(
                    code="missing_anagram_metadata",
                    level="error",
                    message="meta.longestAnagram exists but no anagram item was found in valid_words_metadata",
                    details={"word": longest},
                )
            )
            return issues

        matching = [item for item in anagram_items if _norm(item.get("word")) == longest]
        if not matching:
            issues.append(
                ValidationIssue(
                    code="anagram_metadata_word_mismatch",
                    level="error",
                    message="no anagram metadata word matches meta.longestAnagram",
                    details={"longestAnagram": longest, "anagram_words": [_norm(item.get('word')) for item in anagram_items]},
                )
            )
        if len(anagram_items) > 1:
            issues.append(
                ValidationIssue(
                    code="multiple_anagram_metadata_items",
                    level="error",
                    message="expected exactly one anagram metadata item",
                    details={"count": len(anagram_items)},
                )
            )
        return issues


class AnagramLetterInventoryRule:
    name = "anagram_letter_inventory"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        meta = ctx.meta
        longest = _norm(meta.get("longestAnagram") or meta.get("longest_anagram"))
        if not longest:
            return []
        grid_inventory = Counter("".join(_norm(row) for row in ctx.grid_rows))
        needed = Counter(longest)
        missing = {
            ch: count - grid_inventory.get(ch, 0)
            for ch, count in needed.items()
            if grid_inventory.get(ch, 0) < count
        }
        if not missing:
            return []
        return [
            ValidationIssue(
                code="anagram_letters_missing",
                level="error",
                message="longestAnagram letters are not fully present in the main grid",
                details={"missing": missing, "word": longest},
            )
        ]


class FixedTargetExclusionRule:
    name = "fixed_target_exclusion"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        meta = ctx.meta
        vertical = _norm(meta.get("verticalTargetWord") or meta.get("vertical_target_word"))
        diagonal = _norm(meta.get("diagonalTargetWord") or meta.get("diagonal_target_word"))
        col_idx = _int_or_none(meta.get("columnIndex") or meta.get("column_index"))
        diag_dir = _norm(meta.get("diagonalDirection") or meta.get("diagonal_direction"))
        issues: list[ValidationIssue] = []
        for item in ctx.valid_words_metadata:
            word = _norm(item.get("word"))
            item_type = _norm(item.get("type"))
            direction = _norm(item.get("direction"))
            index = _int_or_none(item.get("index"))

            is_fixed_vertical = (
                vertical
                and item_type == "column"
                and word in {vertical, vertical[::-1]}
                and (col_idx is None or index == col_idx)
            )
            is_fixed_diagonal = (
                diagonal
                and item_type == "diagonal"
                and word in {diagonal, diagonal[::-1]}
                and (not diag_dir or direction in {diag_dir, f"{diag_dir}_rev"})
            )

            if is_fixed_vertical or is_fixed_diagonal:
                issues.append(
                    ValidationIssue(
                        code="fixed_target_word_scored",
                        level="error",
                        message=f"fixed target word appears in fixed target scoring metadata: {word}",
                        details={"item": item},
                    )
                )
        return issues


class SemiCompletedConsistencyRule:
    name = "semi_completed_consistency"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        semi = ctx.semi_completed_payload
        if not semi:
            return []
        meta = ctx.meta
        issues: list[ValidationIssue] = []

        field_pairs = [
            ("date", meta.get("date")),
            ("word_length", meta.get("wordLength")),
            ("vertical_target_word", meta.get("verticalTargetWord")),
            ("column_index", meta.get("columnIndex")),
            ("diagonal_direction", meta.get("diagonalDirection")),
            ("diagonal_target_word", meta.get("diagonalTargetWord")),
            ("longest_anagram_count", meta.get("longestAnagramCount")),
            ("madnessAvailable", meta.get("madnessAvailable")),
            ("difficultyBandType", meta.get("difficultyBandType")),
            ("difficultyBandApplied", meta.get("difficultyBandApplied")),
        ]
        for semi_key, expected in field_pairs:
            if semi.get(semi_key) != expected:
                issues.append(
                    ValidationIssue(
                        code="semi_completed_field_mismatch",
                        level="error",
                        message=f"semi-completed field {semi_key} does not match completed payload metadata",
                        details={"semi": semi.get(semi_key), "completed": expected},
                    )
                )

        semi_rows = semi.get("grid_rows") or []
        completed_rows = ctx.grid_rows
        if len(semi_rows) != len(completed_rows):
            issues.append(
                ValidationIssue(
                    code="semi_completed_grid_shape_mismatch",
                    level="error",
                    message="semi-completed grid row count does not match completed payload",
                )
            )
            return issues
        for r_idx, (semi_row, completed_row) in enumerate(zip(semi_rows, completed_rows)):
            if len(semi_row) != len(completed_row):
                issues.append(
                    ValidationIssue(
                        code="semi_completed_grid_shape_mismatch",
                        level="error",
                        message=f"semi-completed row {r_idx} length does not match completed row length",
                    )
                )
                continue
            for c_idx, (semi_ch, completed_ch) in enumerate(zip(semi_row, completed_row)):
                if semi_ch == "*":
                    continue
                if semi_ch != completed_ch:
                    issues.append(
                        ValidationIssue(
                            code="semi_completed_visible_letter_mismatch",
                            level="error",
                            message=f"semi-completed visible letter mismatch at ({r_idx}, {c_idx})",
                            details={"semi": semi_ch, "completed": completed_ch},
                        )
                    )
        return issues


class BonusAlwaysZeroRule:
    name = "bonus_always_zero"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        issues: list[ValidationIssue] = []
        for idx, item in enumerate(ctx.valid_words_metadata):
            bonus = _int_or_none(item.get("bonus"))
            if bonus is None:
                issues.append(
                    ValidationIssue(
                        code="missing_bonus_field",
                        level="error",
                        message=f"valid_words_metadata[{idx}] is missing an integer bonus field",
                        details={"item": item},
                    )
                )
                continue
            if bonus != 0:
                issues.append(
                    ValidationIssue(
                        code="non_zero_bonus",
                        level="error",
                        message=f"valid_words_metadata[{idx}] has non-zero bonus {bonus}",
                        details={"item": item},
                    )
                )
        return issues


class DifficultyBandConsistencyRule:
    name = "difficulty_band_consistency"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        meta = ctx.meta
        applied = _norm(meta.get("difficultyBandApplied"))
        if not applied:
            return []

        if meta.get("madnessAvailable") is True:
            if applied != "skipped":
                return [
                    ValidationIssue(
                        code="difficulty_band_mismatch",
                        level="error",
                        message=f"madness puzzle should use difficultyBandApplied='skipped', got '{applied}'",
                        details={"difficultyBandApplied": applied, "total_score": ctx.total_score},
                    )
                ]
            return []

        expected = band_for_total_score(ctx.total_score)
        if expected is None:
            return [
                ValidationIssue(
                    code="difficulty_band_unclassifiable",
                    level="error",
                    message=f"could not classify total_score {ctx.total_score} into a difficulty band",
                    details={"total_score": ctx.total_score},
                )
            ]
        if applied != expected:
            return [
                ValidationIssue(
                    code="difficulty_band_mismatch",
                    level="error",
                    message=f"difficultyBandApplied '{applied}' does not match total_score band '{expected}'",
                    details={"difficultyBandApplied": applied, "expected": expected, "total_score": ctx.total_score},
                )
            ]
        return []


class TotalScoreRule:
    name = "total_score"

    def validate(self, ctx: PuzzleValidationContext) -> list[ValidationIssue]:
        computed = 0
        issues: list[ValidationIssue] = []
        for idx, item in enumerate(ctx.valid_words_metadata):
            try:
                item_score = int(item.get("score") or 0)
                computed += item_score
            except Exception:
                issues.append(
                    ValidationIssue(
                        code="invalid_item_score",
                        level="error",
                        message=f"valid_words_metadata[{idx}] has a non-integer score",
                        details={"item": item},
                    )
                )
                continue
            base_score = _int_or_none(item.get("base_score"))
            bonus = _int_or_none(item.get("bonus"))
            if base_score is not None and bonus is not None and item_score != base_score + bonus:
                issues.append(
                    ValidationIssue(
                        code="item_score_mismatch",
                        level="error",
                        message=f"valid_words_metadata[{idx}] score does not match base_score + bonus",
                        details={"item": item},
                    )
                )
        if computed != ctx.total_score:
            issues.append(
                ValidationIssue(
                    code="total_score_mismatch",
                    level="error",
                    message=f"payload total_score is {ctx.total_score}, expected {computed}",
                )
            )
        return issues


def _norm(value: Any) -> str:
    return str(value or "").strip().lower()


def _int_or_none(value: Any) -> int | None:
    try:
        if value is None:
            return None
        return int(value)
    except Exception:
        return None


def _normalize_madness_path(path: Any) -> list[tuple[int, int]] | None:
    if not isinstance(path, list):
        return None
    coords: list[tuple[int, int]] = []
    for step in path:
        row_idx: int | None
        col_idx: int | None
        if isinstance(step, dict):
            row_idx = _int_or_none(step.get("r"))
            col_idx = _int_or_none(step.get("c"))
        elif isinstance(step, (list, tuple)) and len(step) == 2:
            row_idx = _int_or_none(step[0])
            col_idx = _int_or_none(step[1])
        else:
            return None
        if row_idx is None or col_idx is None:
            return None
        coords.append((row_idx, col_idx))
    return coords
