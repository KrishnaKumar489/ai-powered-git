## Usage

Follow the steps below to run the project:

1. **Clone the target repository** that you want to analyze.

2. **Clone this repository** as well.

3. **Run the following command for the first time** to collect repository data and query it:

```bash
python main.py --repo ./your-repo -q "your query here"
```
4. For subsequent queries on the same repository, skip the data collection step using:

```bash
python main.py --repo ./your-repo --skip-collect -q "your query here"
```
