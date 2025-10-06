

# Tools
- configuration
    hydra



# Code
Following tools 
```
uv run black src/
uv run ruff check src/
uv run mypy src/
uv run bandit -r src/
```



| Phase               | Key Tools                                                |
| ------------------- | -------------------------------------------------------- |
| Development         | `uv`, `black`, `ruff`, `mypy`, `pre-commit`, `pytest`    |
| Config              | `hydra`, `pydantic`                                      |
| Orchestration       |                                   |
| Experiment Tracking | `MLflow`                                                 |
| Serving             | `FastAPI`,                             |
| Monitoring          | `Evidently`, `Prometheus`, `Grafana`, `loguru`, `sentry` |
| Deployment          | `docker`, `GitHub Actions`,                   |

