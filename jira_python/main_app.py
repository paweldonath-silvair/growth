import plotly.graph_objects as go
from plotly.offline import plot

from data_utils import IssueDetails
from jira_api import get_issue, search_issues

from figures_utils.table import generate_table
from figures_utils.treemap import generate_treemap, HierarchyLevels


issues = []
issues = search_issues(sprint='Delta 2020 week 13')
figure = generate_treemap(issues, HierarchyLevels.EPIC)
plot(figure, auto_open=True, filename='test.html')
