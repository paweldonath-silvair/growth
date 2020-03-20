import operator
from enum import Enum
from functools import reduce
from typing import List, Any, Dict, Optional

from data_utils import IssueDetails


class AbstractLevel:
    def __init__(self, issue: IssueDetails):
        self.issue = issue

    @property
    def id(self) -> str:
        raise NotImplemented

    @property
    def label(self) -> str:
        raise NotImplemented

    @property
    def value(self) -> float:
        return 1.0

    @property
    def text(self) -> Optional[str]:
        return None


class LevelIssue(AbstractLevel):
    @property
    def id(self) -> str:
        return self.issue.key

    @property
    def label(self) -> Optional[str]:
        return self.issue.key

    @property
    def text(self) -> Optional[str]:
        return self.issue.summary


class LevelEpic(AbstractLevel):
    @property
    def id(self) -> str:
        return self.issue.epic

    @property
    def label(self) -> str:
        return self.issue.epic_name


class LevelComponent(AbstractLevel):
    @property
    def id(self) -> str:
        return self.issue.main_component

    @property
    def label(self) -> str:
        return self.issue.main_component


class HierarchyLevels(Enum):
    COMPONENT = (LevelComponent, LevelEpic, LevelIssue)
    EPIC = (LevelEpic, LevelComponent, LevelIssue)


def generate_treemap(issues: List[IssueDetails], hierarchy_levels: HierarchyLevels) -> Dict[str, Any]:
    hierarchy_len = len(hierarchy_levels.value)

    levels_ids = [[] for _ in range(hierarchy_len)]
    levels_parents = [[] for _ in range(hierarchy_len)]
    levels_labels = [[] for _ in range(hierarchy_len)]
    levels_texts = [[] for _ in range(hierarchy_len)]
    levels_values = [[] for _ in range(hierarchy_len)]

    for issue in issues:
        partial_ids = [level(issue).id for level in hierarchy_levels.value]
        full_ids = ["-".join(partial_ids[:n+1]) for n in range(hierarchy_len)]
        parents = [""] + full_ids[:-1]
        labels = [level(issue).label for level in hierarchy_levels.value]
        texts = [level(issue).text for level in hierarchy_levels.value]
        values = [level(issue).value for level in hierarchy_levels.value]

        for n in range(hierarchy_len):
            if full_ids[n] not in levels_ids[n]:
                levels_ids[n].append(full_ids[n])
                levels_parents[n].append(parents[n])
                levels_labels[n].append(labels[n])
                levels_texts[n].append(texts[n])
                levels_values[n].append(values[n])

    all_ids = reduce(operator.add, levels_ids, [])
    all_parents = reduce(operator.add, levels_parents, [])
    all_labels = reduce(operator.add, levels_labels, [])
    all_texts = reduce(operator.add, levels_texts, [])
    all_values = reduce(operator.add, levels_values, [])

    data = dict(
        type="treemap",
        textinfo="label+text",
        ids=all_ids,
        parents=all_parents,
        labels=all_labels,
        text=all_texts,
        # values=all_values,
        # branchvalues="remainder",
    )

    layout = dict(
        title="Treemap"
    )

    figure = dict(
        data=[data],
        layout=layout
    )
    return figure
