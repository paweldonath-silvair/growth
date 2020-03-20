import json
from dataclasses import dataclass
from datetime import datetime
from typing import Dict, Any, List, Optional


class FieldsTranslator:
    _fields_id_to_name = {}

    @staticmethod
    def _read_fields_id_to_name(filename='fields_id_to_name.json'):
        with open(filename) as f:
            data = json.load(f)
        return data

    @classmethod
    def get(cls):
        if cls._fields_id_to_name is not None:
            cls._fields_id_to_name = cls._read_fields_id_to_name()
        return cls._fields_id_to_name


@dataclass
class StatusCategory:
    id: int
    key: str
    name: str
    color: str

    @classmethod
    def from_dict(cls, d):
        return cls(
            id=int(d['id']),
            key=d['key'],
            name=d['name'],
            color=d['colorName'],
        )


@dataclass
class Status:
    id: int
    name: str
    category: StatusCategory

    @classmethod
    def from_dict(cls, d):
        return cls(
            id=int(d['id']),
            name=d['name'],
            category=StatusCategory.from_dict(d['statusCategory']),
        )


@dataclass
class IssueShort:
    key: str
    summary: str
    status: Status

    @classmethod
    def from_dict(cls, d):
        return cls(
            key=d['key'],
            summary=d['fields']['summary'],
            status=Status.from_dict(d['fields']['status']),
        )


@dataclass
class LinkingType:
    name: str
    direction: str
    description: str


@dataclass
class LinkedIssue:
    type: LinkingType
    issue: IssueShort

    @classmethod
    def from_dict(cls, d):
        direction = 'inward' if 'inwardIssue' in d else 'outward'
        return cls(
            type=LinkingType(
                name=d['type']['name'],
                direction=direction,
                description=d['type'][direction],
            ),
            issue=IssueShort.from_dict(d[f'{direction}Issue']),
        )


@dataclass
class IssueType:
    type_name: str
    is_subtask: bool

    @classmethod
    def from_dict(cls, d):
        return cls(
            type_name=d['name'],
            is_subtask=bool(d['subtask']),
        )


@dataclass
class Project:
    key: str
    name: str

    @classmethod
    def from_dict(cls, d):
        return cls(
            key=d['key'],
            name=d['name'],
        )


@dataclass
class Sprint:
    name: str
    state: str

    @classmethod
    def from_dict(cls, d):
        return cls(
            name=d['name'],
            state=d['state'],
        )

    @classmethod
    def from_str(cls, s: str):
        s = s[s.find('[') + 1:-1]
        fields = [f.split('=') for f in s.split(',')]
        d = {f[0]: f[1] for f in fields}
        return cls.from_dict(d)


@dataclass
class IssueDetails(IssueShort):
    resolved: Optional[datetime]
    type: IssueType
    story_points: Optional[float]
    project: Project
    epic: Optional[str]
    components: List[str]
    sprints: List[Sprint]
    labels: List[str]
    linked_issues: List[LinkedIssue]
    sub_tasks: List[IssueShort]
    url: str

    active_sprint_name: Optional[str] = None
    main_component: Optional[str] = None
    epic_name: Optional[str] = None

    def __post_init__(self):
        active_sprints = [s for s in self.sprints if s.state == 'ACTIVE']
        if len(active_sprints) > 0:
            self.active_sprint_name = active_sprints[0].name
        else:
            self.active_sprint_name = "NoActiveSprint"

        if len(self.components) > 0:
            self.main_component = self.components[0]
        else:
            self.main_component = "NoComponent"

    @classmethod
    def get_epic_name(cls, epic, fields_id_to_name=None):
        if fields_id_to_name is None:
            fields_id_to_name = FieldsTranslator.get()
        fields = {fields_id_to_name[k]: v for k, v in epic.raw['fields'].items()}
        return fields.get('Epic Name')

    @classmethod
    def from_jira_issue(cls, issue, fields_id_to_name=None):
        if fields_id_to_name is None:
            fields_id_to_name = FieldsTranslator.get()
        fields = {fields_id_to_name[k]: v for k, v in issue.raw['fields'].items()}
        if fields.get('Sprint') is None:
            fields['Sprint'] = []
        return cls(
            key=issue.key,
            summary=fields.get('Summary'),
            status=Status.from_dict(fields.get('Status')),
            resolved=fields.get('Resolved'),
            type=IssueType.from_dict(fields.get('Issue Type')),
            story_points=float(fields.get('Story Points')) if fields.get('Story Points') is not None else None,
            project=Project.from_dict(fields.get('Project')),
            epic=fields.get('Epic Link'),
            components=[c['name'] for c in fields.get('Components')],
            sprints=[Sprint.from_str(s) for s in fields.get('Sprint')],
            labels=fields.get('Labels'),
            linked_issues=[LinkedIssue.from_dict(i) for i in fields.get('Linked Issues')],
            sub_tasks=[IssueShort.from_dict(s) for s in fields.get('Sub-tasks')],
            url=issue.permalink()
        )




