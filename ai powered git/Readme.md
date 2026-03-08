## Usage

Follow the steps below to run the project:

1. **Clone the target repository** that you want to analyze.

2. **Clone this repository** as well.

3. **Configure AWS Credentials**
This project uses AWS Bedrock, so you must configure AWS credentials.
Run:
```bash
aws configure
 ```
Enter the following when prompted:
```bash
AWS Access Key ID: <your-access-key>
AWS Secret Access Key: <your-secret-key>
Default region name: us-east-1
Default output format: json
```
4. **Run the following command for the first time** to collect repository data and query it:

```bash
python main.py --repo ./your-repo -q "your query here"
```
5. For subsequent queries on the same repository, skip the data collection step using:

```bash
python main.py --repo ./your-repo --skip-collect -q "compare main and develop"
```


## IMPORTANT
The current ai powered git system only works for 5 programming languages: python, java, javascript, html, css. SInce its an MVP we have decided to start with 5 programming languages.

python main.py --repo ./your-repo --skip-collect -q "your query here"
```
