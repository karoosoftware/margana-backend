from __future__ import annotations

from typing import Any


def build_valid_word_items(
    *,
    grid_rows: list[str],
    words5: list[str] | set[str],
    diagonal_words: list[str] | set[str],
    diagonal_lengths: set[int],
    include_coordinates: bool,
) -> list[dict[str, Any]]:
    items: list[dict[str, Any]] = []
    row_count = len(grid_rows)
    col_count = len(grid_rows[0]) if row_count > 0 else 0
    words5_set = {str(word).lower() for word in words5}
    diagonal_words_set = {str(word).lower() for word in diagonal_words}

    def _item(
        *,
        word: str,
        item_type: str,
        index: int,
        direction: str,
        start: tuple[int, int] | None = None,
        end: tuple[int, int] | None = None,
    ) -> dict[str, Any]:
        result: dict[str, Any] = {
            "word": word,
            "type": item_type,
            "index": index,
            "direction": direction,
        }
        if include_coordinates:
            result["start_index"] = None if start is None else {"r": start[0], "c": start[1]}
            result["end_index"] = None if end is None else {"r": end[0], "c": end[1]}
        return result

    rows_lr = [str(row).lower() for row in grid_rows]
    rows_rl = [row[::-1] for row in rows_lr]
    for idx, word in enumerate(rows_lr):
        if word in words5_set:
            items.append(
                _item(
                    word=word,
                    item_type="row",
                    index=idx,
                    direction="lr",
                    start=(idx, 0),
                    end=(idx, max(0, col_count - 1)),
                )
            )
    for idx, word in enumerate(rows_rl):
        if word in words5_set and not (idx < len(rows_lr) and word == rows_lr[idx]):
            items.append(
                _item(
                    word=word,
                    item_type="row",
                    index=idx,
                    direction="rl",
                    start=(idx, max(0, col_count - 1)),
                    end=(idx, 0),
                )
            )

    cols_tb: list[str] = []
    cols_bt: list[str] = []
    for col_idx in range(col_count):
        col_word = "".join(grid_rows[row_idx][col_idx] for row_idx in range(row_count)).lower()
        cols_tb.append(col_word)
        cols_bt.append(col_word[::-1])
    for idx, word in enumerate(cols_tb):
        if word in words5_set:
            items.append(
                _item(
                    word=word,
                    item_type="column",
                    index=idx,
                    direction="tb",
                    start=(0, idx),
                    end=(max(0, row_count - 1), idx),
                )
            )
    for idx, word in enumerate(cols_bt):
        if word in words5_set and not (idx < len(cols_tb) and word == cols_tb[idx]):
            items.append(
                _item(
                    word=word,
                    item_type="column",
                    index=idx,
                    direction="bt",
                    start=(max(0, row_count - 1), idx),
                    end=(0, idx),
                )
            )

    def on_edge(row_idx: int, col_idx: int) -> bool:
        return row_idx == 0 or col_idx == 0 or row_idx == row_count - 1 or col_idx == col_count - 1

    def add_diagonal_items(paths: list[list[tuple[int, int]]], forward_dir: str, reverse_dir: str) -> None:
        for path in paths:
            letters = "".join(grid_rows[row_idx][col_idx] for row_idx, col_idx in path).lower()
            for start_idx in range(len(path)):
                for end_idx in range(start_idx + 1, len(path)):
                    seg_len = end_idx - start_idx + 1
                    if seg_len not in diagonal_lengths:
                        continue
                    start = path[start_idx]
                    end = path[end_idx]
                    if not (on_edge(start[0], start[1]) and on_edge(end[0], end[1])):
                        continue
                    word_forward = letters[start_idx:end_idx + 1]
                    word_reverse = word_forward[::-1]
                    if word_forward in diagonal_words_set:
                        items.append(
                            _item(
                                word=word_forward,
                                item_type="diagonal",
                                index=0,
                                direction=forward_dir,
                                start=start,
                                end=end,
                            )
                        )
                    if word_reverse in diagonal_words_set and word_reverse != word_forward:
                        items.append(
                            _item(
                                word=word_reverse,
                                item_type="diagonal",
                                index=0,
                                direction=reverse_dir,
                                start=end,
                                end=start,
                            )
                        )

    main_paths: list[list[tuple[int, int]]] = []
    anti_paths: list[list[tuple[int, int]]] = []
    for col_idx in range(col_count):
        path: list[tuple[int, int]] = []
        row_idx, cur_col_idx = 0, col_idx
        while row_idx < row_count and cur_col_idx < col_count:
            path.append((row_idx, cur_col_idx))
            row_idx += 1
            cur_col_idx += 1
        if len(path) >= 2:
            main_paths.append(path)
    for row_start in range(1, row_count):
        path = []
        row_idx, col_idx = row_start, 0
        while row_idx < row_count and col_idx < col_count:
            path.append((row_idx, col_idx))
            row_idx += 1
            col_idx += 1
        if len(path) >= 2:
            main_paths.append(path)
    for col_start in range(col_count - 1, -1, -1):
        path = []
        row_idx, col_idx = 0, col_start
        while row_idx < row_count and col_idx >= 0:
            path.append((row_idx, col_idx))
            row_idx += 1
            col_idx -= 1
        if len(path) >= 2:
            anti_paths.append(path)
    for row_start in range(1, row_count):
        path = []
        row_idx, col_idx = row_start, col_count - 1
        while row_idx < row_count and col_idx >= 0:
            path.append((row_idx, col_idx))
            row_idx += 1
            col_idx -= 1
        if len(path) >= 2:
            anti_paths.append(path)

    add_diagonal_items(main_paths, "main", "main_rev")
    add_diagonal_items(anti_paths, "anti", "anti_rev")
    return items
