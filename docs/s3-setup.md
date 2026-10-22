# S3 setup (notebook Step 5)

Step 5 of the notebook exports the Gold tables and raw rides to S3 as CSV files under
`ride-analytics/<YYYY-MM-DD_HH-MM>/`.

## 1. Billing safety
Go to **Billing → Budgets → Create a zero-spend budget** with an email alert.

## 2. Create the bucket
**S3 → Create bucket**. Use a globally unique name, for example `ride-analytics-<your-suffix>`, in your
preferred region (for example `ap-south-1`). Keep **Block all public access** turned on.

## 3. Create an IAM user with least-privilege access
Create an IAM user (for example `databricks-s3`) with this inline policy:

```json
{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": ["s3:PutObject", "s3:GetObject", "s3:ListBucket"],
    "Resource": ["arn:aws:s3:::YOUR-BUCKET-NAME", "arn:aws:s3:::YOUR-BUCKET-NAME/*"]
  }]
}
```

Go to **Security credentials → Create access key → Application running outside AWS**.

## 4. Configure the notebook
Fill in `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` and `S3_BUCKET` in the Step 0 cell.

> **Never commit real keys.** The `.gitignore` already excludes `.env`, `*credentials*` and
> `*accessKeys*.csv`. A better option is to store the keys in Databricks secrets
> (`dbutils.secrets.get(...)`) instead of in the notebook.
